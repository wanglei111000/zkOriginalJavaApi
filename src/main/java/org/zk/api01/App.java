package org.zk.api01;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;

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


        /**
         * 同步创建一个持久节点,ACL为 world:anyone:cdrwa 等同于如下命令：
         * create /node 123 world:anyone:cdrwa
         */
        zookeeper.create("/node", "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        /**
         * 同步创建一个持久节点，ACL为 world:anyone:cdrwa 所有人只拥有创建的权限,等同于如下命令：
         * create /node1 123 world:anyone:c
         */
        zookeeper.create("/node1",
                "123".getBytes(),
                Collections.singletonList(new ACL(ZooDefs.Perms.CREATE, ZooDefs.Ids.ANYONE_ID_UNSAFE)),
                CreateMode.PERSISTENT);


        /**
         * 异步创建一个 临时的顺序节点，ACL为 ip:192.168.217.128:c 等同于如下命令：
         * create /node2 123 ip:192.168.217.128:c
         */
        zookeeper.create("/node2",
                "123".getBytes(),
                Collections.singletonList(new ACL(ZooDefs.Perms.CREATE, new Id("ip", "192.168.217.128"))),
                CreateMode.EPHEMERAL_SEQUENTIAL,
                new AsyncCallback.StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        System.out.println("rc:" + rc);
                        System.out.println("path:" + path);
                        System.out.println("ctx:" + ctx);
                        System.out.println("name:" + name);
                    }
                }, "传给服务端的内容,会在异步回调时传回来");
        /**
         * 注意这里,线程睡眠20秒,因为是创建的临时节点,如果不睡眠,你不能使用命令在控制台看见创建的临时节点
         */
        Thread.sleep(20000);


        /**
         * 异步创建一个持久节点, ACL为 digest:user:G2RdrM8e0u0f1vNCj/TI99ebRMw=:cdrwa,等同于如下命令：
         * create /node3 123 digest:user:G2RdrM8e0u0f1vNCj/TI99ebRMw=:cdrwa
         */

//        zookeeper.create("/node3",
//                "123".getBytes(),
//                Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("digest", "user:G2RdrM8e0u0f1vNCj/TI99ebRMw="))),
//                CreateMode.PERSISTENT,
//                new AsyncCallback.StringCallback() {
//                    @Override
//                    public void processResult(int rc, String path, Object ctx, String name) {
//                        System.out.println("rc:" + rc);
//                        System.out.println("path:" + path);
//                        System.out.println("ctx:" + ctx);
//                        System.out.println("name:" + name);
//                    }
//                }, "传给服务端的内容,会在异步回调时传回来");
//        /**
//         * 注意这里,线程睡眠20秒,可以接收到watcher
//         */
//        Thread.sleep(20000);

        /**
         * 异步调用
         * path 节点路径
         * watch true使用创建zookeeper时指定的默认watcher 如果为false则不设置监听
         * DataCallback 异步通知
         * ctx 回调上下文
         */
        zookeeper.getData("/node3", false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("rc:" + rc);
                System.out.println("path:" + path);
                System.out.println("ctx:" + ctx);
                System.out.println("data:" + new String(data));
                System.out.println("stat:" + stat);
            }
        },"传给服务端的内容,会在异步回调时传回来");
        Thread.sleep(2000);


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
