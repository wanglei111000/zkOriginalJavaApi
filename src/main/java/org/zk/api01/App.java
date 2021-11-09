package org.zk.api01;

import org.apache.zookeeper.*;

import java.io.IOException;

public class App implements Watcher
{
    public static void main( String[] args ) throws IOException, InterruptedException, KeeperException {
        String connStr = "192.168.217.128:2181,192.168.217.135:2181,192.168.217.136:2181";
        ZooKeeper zookeeper = new ZooKeeper(connStr , 5000, new App());
        System.out.println(zookeeper.getState());
        Thread.sleep(3000);
        System.out.println(zookeeper.getState());
        String result = "";

        // 创建一个持久化节点 必须现在zk 中先创建一个父级目录
        //如果没有多个层级 则不需要
        result = zookeeper.create("/test001", "angelaBaby I love you".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(result);
        Thread.sleep(10);

        result = zookeeper.create("/top/api", "angelaBaby I love you".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(result);
        Thread.sleep(10);
        //报错

        // 获取节点的值
        byte[] bs = zookeeper.getData("/top/api", true, null);
        result = new String(bs);
        System.out.println("/top/api节点的数据是:" + result);
        Thread.sleep(10);

        // 修改节点
        zookeeper.setData("/top/api", "I love you yangmi".getBytes(), -1);
        Thread.sleep(10);
        //和获取修改后的值
        bs = zookeeper.getData("/top/api", true, null);
        result = new String(bs);
        System.out.println("修改/top/api节点后的数据是:" + result);
        Thread.sleep(10);

        // 删除节点
        System.out.println("节点删除开始");
        zookeeper.delete("/top/api", -1);
        System.out.println("节点删除成功");

    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if(event.getState() == Event.KeeperState.SyncConnected){
            System.out.println("zookeeper state is " + Event.KeeperState.SyncConnected);
            System.out.println("coonection is successful");
        }
    }
}
