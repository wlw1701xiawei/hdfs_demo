package cn.hut.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {
    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;  //2秒
    private ZooKeeper zkClient;

    @BeforeEach
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                List<String> children;
                try {
                    children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/China", "CN".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    //获取子节点并监控数据的变化
    @Test
    public void getDataAndWatch() throws InterruptedException {
//        List<String> children = zkClient.getChildren("/", true);
//        for (String child : children) {
//            System.out.println(child);
//        }
        Thread.sleep(Long.MAX_VALUE);
    }

    //判断节点是否存在
    @Test
    public void isExistNode() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/China", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
