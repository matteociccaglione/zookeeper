package org.apache.zookeeper.client;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.ClientInfo;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
@Ignore
@RunWith(Parameterized.class)
public class ZookeeperAuthTest extends ZookeeperTestBaseClass{
    private String scheme;
    private String auth;
    public ZookeeperAuthTest(String scheme, String auth){
        this.portNumber=2222;
        this.directoryName = "zookeeperAuth";
        this.scheme = scheme;
        this.auth = auth;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][] {
                {"digest","user:pass"},
                {"sasl","user:pass"},
                {"world","user:pass"}
        });
    }

    @Test
    public void testAddAuth() throws NoSuchAlgorithmException, InterruptedException {
        this.client.addAuthInfo(this.scheme,this.auth.getBytes(StandardCharsets.UTF_8));
        //Now verify that the auth info are properly set
        //From docs i know that i have to build a Base64 encoding of the auth string
        /*
        if(this.scheme=="digest") {
            String base64 = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA1").digest("user:pass".getBytes()));
        }
        */
        List< ClientInfo> list = this.client.whoAmI();
        Assert.assertEquals(this.scheme,list.get(list.size()-1).getAuthScheme());
        //ACL acl = new ACL(ZooDefs.Perms.ALL, )
    }
}
