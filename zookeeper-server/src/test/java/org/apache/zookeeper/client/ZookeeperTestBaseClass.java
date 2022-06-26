package org.apache.zookeeper.client;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

public class ZookeeperTestBaseClass {
    protected ZooKeeper client;
    protected ServerCnxnFactory factory;
    protected String directoryName;
    protected int portNumber;
    private static void cleanDirectory(File dir){
        File[] files = dir.listFiles();
        if(files==null){
            return;
        }
        if(files.length!=0){
            for (File file: files){
                if(file.isFile()){
                    file.delete();
                }
                else if (file.isDirectory()){
                    cleanDirectory(file);
                }
            }
        }
        dir.delete();
    }

    @Before
    public void createServer() throws IOException, InterruptedException {
        this.client=null;
        System.out.println("Before method");
        int tickTime = 2000;
        int numConnections = 5000;
        String dataDirectory = System.getProperty("java.io.tmpdir");

        File dir = new File(dataDirectory, this.directoryName).getAbsoluteFile();
        cleanDirectory(dir);
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        ServerCnxnFactory standaloneServerFactory = ServerCnxnFactory.createFactory(portNumber, numConnections);
        System.out.println("Factory created");
        int zkPort = standaloneServerFactory.getLocalPort();
        standaloneServerFactory.startup(server);
        System.out.println("Server startup");
        this.factory = standaloneServerFactory;
        String connection = "127.0.0.1:"+String.valueOf(portNumber);
        System.out.println(connection);
        this.client = new ZooKeeper(connection, 2000, event -> {
            //do something with the event processed
        });

    }
    @After
    public void removeServer(){
        System.out.println("After method");
        try {
            this.client.close();
            this.factory.shutdown();
            String dataDirectory = System.getProperty("java.io.tmpdir");

            File dir = new File(dataDirectory, directoryName).getAbsoluteFile();
            dir.delete();
        }catch(Exception e){

        }
    }
}
