package cn.hut.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //监听完后就会进入到process()方法，调用getChildren()方法再注册，一直监听下去
                //不在这调用getChildren()方法，只会监听一次
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);
        ArrayList<String> hosts = new ArrayList<>();    //存储服务器节点主机名集合
        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        //将所有在线的服务器主机名打印出来
        System.out.println(hosts);
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) {
        DistributeClient client = new DistributeClient();
        try {
            //获取zookeeper集群连接
            client.getConnect();
            //注册监听
            client.getChildren();
            //业务逻辑
            client.business();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
