package org.apache.zookeeper.client;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class ZookeeperClientServerIT {
    private String port;

    public ZookeeperClientServerIT(String port){
        this.port = port;
    }

    @Parameterized.Parameters
    public static Collection configure(){
        return Arrays.asList(new Object[][]{
                {"12352"}
        });
    }

    @Test
    public void testConnectionClientServer() throws IOException, InterruptedException, KeeperException {
        int tickTime = 2000;
        int numConnections = 5000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, "zookeeperit").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        ServerCnxnFactory standaloneServerFactory = Mockito.spy(ServerCnxnFactory.createFactory(Integer.parseInt(this.port), numConnections));
        standaloneServerFactory.startup(server);
        ZooKeeper client = new ZooKeeper("127.0.0.1:"+this.port,2000,event -> {

        });
        Iterable<ServerCnxn> conns = standaloneServerFactory.getConnections();
        for (ServerCnxn conn: conns) {
            Mockito.verify(standaloneServerFactory, Mockito.times(1)).registerConnection(conn);
        }
        client.close();
        standaloneServerFactory.shutdown();
    }

    @Test
    public void testNumberConnectionClientServer() throws IOException, InterruptedException, KeeperException {
        int tickTime = 2000;
        int numConnections = 5000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, "zookeeperit").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(Integer.parseInt(this.port)+1, numConnections);
        standaloneServerFactory.startup(server);
        int numConnection = standaloneServerFactory.getNumAliveConnections();
        int port = Integer.parseInt(this.port)+1;
        ZooKeeper client = new ZooKeeper("127.0.0.1:"+String.valueOf(port),2000,event -> {

        });
        client.exists("/",false);
        int numConnFinal = standaloneServerFactory.getNumAliveConnections();
        client.close();
        standaloneServerFactory.shutdown();
        Assert.assertEquals(numConnection+1,numConnFinal);
    }

    @Test
    public void testWithPassword() throws IOException, InterruptedException {
        int tickTime = 2000;
        int numConnections = 5000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, "zookeeperit").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(Integer.parseInt(this.port)+1, numConnections);
        standaloneServerFactory.startup(server);
        int numConnection = standaloneServerFactory.getNumAliveConnections();
        int port = Integer.parseInt(this.port)+2;
        ZooKeeper client = new ZooKeeper("127.0.0.1:"+String.valueOf(port),2000,event -> {

        },100, "this_is_a_password".getBytes(StandardCharsets.UTF_8));
        byte[] sessPasswd = client.getSessionPasswd();
        byte[] originalPasswd = "this_is_a_password".getBytes(StandardCharsets.UTF_8);
        boolean isCorrect = originalPasswd.length==sessPasswd.length;
        if(isCorrect) {
            for (int i = 0; i < originalPasswd.length; i++) {
                if(originalPasswd[i]!=sessPasswd[i]){
                    isCorrect=false;
                    break;
                }
            }
        }
        client.close();
        standaloneServerFactory.shutdown();
        Assert.assertTrue(isCorrect);
    }

}
