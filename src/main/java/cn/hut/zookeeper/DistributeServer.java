package cn.hut.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {
    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online");
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) {
        DistributeServer server = new DistributeServer();
        try {
            //连接zookeeper集群
            server.getConnect();
            //注册节点
            server.regist(args[0]);
            //业务逻辑
            server.business();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
