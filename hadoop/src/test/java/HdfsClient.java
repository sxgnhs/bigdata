/**
 * @author hs
 * @date 2021/5/29 10:19 上午
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码常用套路
 * 1 获取客户端对象
 * 2 执行相关操作命令
 * 3 关闭资源
 * hdfs  zookeeper
 */
public class HdfsClient {

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        // 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
        // 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
        // new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
        // 然后再加载classpath下的hdfs-site.xml
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop12:8020");
        /**
         * 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
         * 从低到高
         * hdfs-default.xml => hdfs-site.xml => 项目资源目录下的配置文件 => 代码中设置的值
         *
         */
        conf.set("dfs.replication", "3");
        // 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
        // fs = FileSystem.get(conf);

        // 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
        fs = FileSystem.get(new URI("hdfs://hadoop12:8020"), conf, "root");
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 创建文件
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop12:8020");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(uri, configuration, "root");
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
        fs.close();
    }

    /**
     * 往hdfs上传文件
     *
     * @throws Exception
     */
    @Test
    public void testAddFileToHdfs() throws Exception {

        // 要上传的文件所在的本地路径
        Path src = new Path("/Users/hushai/Documents/project/bigdata/test.txt");
        // 要上传到hdfs的目标路径
        Path dst = new Path("/xiyou/huaguoshan");
        // 参数解读: 参数一表示删除原数据(如果为true,本地文件上传以后会删除),参数二,是否允许覆盖 参数三 原数据路径,参数四,目的地路径
        fs.copyFromLocalFile(false, false, src, dst);
    }

    /**
     * 从hdfs中复制文件到本地文件系统
     * 下载
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
        //参数解读 参数一 : 源文件是否删除, 参数二 源文件路径 hdfs路径  参数三: 目标地址路径:本地路径 参数四:是否开启本地校验
        //参数四 校验方式crc校验,类似MD5
        fs.copyToLocalFile(new Path("/xiyou/huaguoshan"), new Path("./test/"));
    }

    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {

        // 创建目录
        fs.mkdirs(new Path("/a1/b1/c1"));

        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true

        fs.delete(new Path("/aaa"), true);

        // 重命名文件或文件夹参数一 原文件目录,参数二 目标文件目录
        fs.rename(new Path("/a1"), new Path("/a2"));

    }

    /**
     * 查看目录信息，只显示文件
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

        // 思考：为什么返回迭代器，而不是List之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------为angelababy打印的分割线--------------");
        }
    }


    /**
     * 查看文件及文件夹信息
     *
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile()) flag = "f--         ";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }
}
