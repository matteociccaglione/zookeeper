package org.apache.zookeeper.server.auth;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.matchers.Null;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DigestAuthenticationProviderTest {
    private String stringToHash;
    private DigestAuthenticationProvider provider;
    private Type type;
    private String secondString;
    enum Type{
        TEST_DIGEST,
        TEST_DIGEST_NP,
        TEST_IS_V,
        TEST_IS_NV,
        TEST_IS_V_NP,
        TEST_MATCH,
        TEST_NOMATCH,
        TEST_GETUSER,
        TEST_GETUSERFAIL
    }

    public DigestAuthenticationProviderTest(String stringToHash,Type type, String secondString){
        this.stringToHash = stringToHash;
        this.provider = new DigestAuthenticationProvider();
        this.type = type;
        this.secondString=secondString;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][]{
                {"test:hash",Type.TEST_DIGEST,""},
                {"",Type.TEST_DIGEST,""},
                {null,Type.TEST_DIGEST_NP,""},
                {"test:valid",Type.TEST_IS_V,""},
                {"testinvalid",Type.TEST_IS_NV,""},
                {"",Type.TEST_IS_NV,""},
                {null,Type.TEST_IS_V_NP,""},
                /*
                {"user:pass",Type.TEST_GETUSER,"user"},
                {"user:pass",Type.TEST_MATCH,"user:pass"},
                {"",Type.TEST_NOMATCH,"test"},
                {"",Type.TEST_GETUSERFAIL,"user"},
                */

        });
    }


    @Test
    public void testDigest() throws NoSuchAlgorithmException {
        Assume.assumeTrue(type==Type.TEST_DIGEST);
        byte[] digest = DigestAuthenticationProvider.digest(this.stringToHash);
        byte[] oracleDigest = MessageDigest.getInstance("SHA1").digest(this.stringToHash.getBytes(StandardCharsets.UTF_8));
        boolean isCorrect = digest.length==oracleDigest.length;
        if(isCorrect){
            for(int i = 0; i < digest.length; i++){
                if(digest[i]!=oracleDigest[i]){
                    isCorrect=false;
                    break;
                }
            }
        }
        Assert.assertTrue(isCorrect);
    }

    @Test(expected = NullPointerException.class)
    public void testDigestNullPoint() throws NoSuchAlgorithmException {
        Assume.assumeTrue(type==Type.TEST_DIGEST_NP);
        DigestAuthenticationProvider.digest(this.stringToHash);
    }

    @Test
    public void testHandleAuthentication() throws IOException, InterruptedException, KeeperException {
        Assume.assumeTrue(type==Type.TEST_DIGEST);
        int tickTime = 2000;
        int numConnections = 5000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, "zookeeperAuthTest").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        int port = 56789;
        ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(port, numConnections);
        standaloneServerFactory.startup(server);
        int numConnection = standaloneServerFactory.getNumAliveConnections();

        ZooKeeper client = new ZooKeeper("127.0.0.1:"+String.valueOf(port),2000, event -> {

        });
        client.exists("/",false);
        Iterable<ServerCnxn> connections = standaloneServerFactory.getConnections();
        ServerCnxn cnxnTest = null;
        for (ServerCnxn cnxn: connections){
            cnxnTest = cnxn;
            break;
        }
        KeeperException.Code code = this.provider.handleAuthentication(cnxnTest,this.stringToHash.getBytes(StandardCharsets.UTF_8));
        client.close();
        standaloneServerFactory.shutdown();
        Assert.assertEquals(KeeperException.Code.OK,code);
    }

    @Test
    public void testIsValid(){
        Assume.assumeTrue(type==Type.TEST_IS_V || type==Type.TEST_IS_NV);
        if(type==Type.TEST_IS_V)
            Assert.assertTrue(this.provider.isValid(this.stringToHash));
        else
            Assert.assertFalse(this.provider.isValid(this.stringToHash));
    }

    @Test(expected = NullPointerException.class)
    public void testIsValidNP(){
        Assume.assumeTrue(type==Type.TEST_IS_V_NP);
        this.provider.isValid(this.stringToHash);
    }

    @Test
    public void testMatches(){
        Assume.assumeTrue(type==Type.TEST_MATCH);
        Assert.assertTrue(this.provider.matches(this.stringToHash,this.secondString));
    }

    @Test
    public void testNoMatches(){
        Assume.assumeTrue(type==Type.TEST_NOMATCH);
        Assert.assertFalse(this.provider.matches(this.stringToHash,this.secondString));
    }

    @Test
    public void testGetUsername(){
        Assume.assumeTrue(type==Type.TEST_GETUSER);
        String result = this.provider.getUserName(this.stringToHash);
        Assert.assertEquals(this.secondString,result);
    }

    @Test
    public void testGetUserFail(){
        Assume.assumeTrue(type==Type.TEST_GETUSERFAIL);
        String result = this.provider.getUserName(this.stringToHash);
        Assert.assertNotEquals(this.secondString, result);
    }

}
