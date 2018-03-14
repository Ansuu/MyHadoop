package org.ansu.HDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import java.io.*;
import java.net.URI;
public class App {
    private Configuration configuration;
    private void HDFS2Local(String hdfsSrc,String localSrc) throws Exception{
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream fsdis = fs.open(new Path(hdfsSrc));
        FileOutputStream fos = new FileOutputStream(localSrc);
        byte[] buffer = new byte[8192];
        int len = 0;
        while ((len = fsdis.read(buffer)) != -1){
            fos.write(buffer,0,len);
        }
        fsdis.close();
        fos.close();
        fs.close();
    }
    /**
     *  hdfs 复制到本地 读取文本行
     * @param hdfsSrc
     * @param localSrc
     */
    private void HDFS2LocalText(String hdfsSrc,String localSrc){
        // 定义文件路径
        String filePath = hdfsSrc;
        try{
            // 实例化一个FileSystem 对象
            FileSystem fs = FileSystem.get(URI.create(filePath),configuration);
            // 定义输出流
            FSDataInputStream in = null;
            try {
                // 将fs 调用Open方法的到的文件流复制给In
                in = fs.open(new Path(filePath));
                // 定义带缓存的Input流
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                //定义文件输出流
                Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localSrc)));
                // 如果能读到 那就输出
                while (br.readLine() !=null){
                    writer.write(br.readLine());
                }
                br.close();
                writer.close();
            }finally {

                // 关闭流
                IOUtils.closeStream(in);
            }
        }catch (IOException e){
            System.exit(-1);
        }
    }
    /**
     * 读取hdfs文件输出到控制台 输出2次
     * @param path
     * @throws Exception
     */
    private void HDFS2Console(String path) throws Exception{
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream in = fileSystem.open(new Path(path));
        byte[] buffer = new byte[4096];
        int len = 0;
        while ((len = in.read(buffer)) != -1){
            System.out.println(new String(buffer,0,len));
        }
        in.seek(0);
        IOUtils.copyBytes(in,System.out,4096,true);
    }
    /**
     * hdfs 创建文件夹
     * @param path
     * @throws Exception
     */
    private void HDFSMkdir(String path) throws Exception{
        FileSystem fs = FileSystem.get(configuration);
        Path in = new Path(path);
        if (fs.exists(in)){
            System.out.println("目录已存在");
            System.exit(-1);
        }
        fs.mkdirs(in);
        FileStatus fileStatus = fs.getFileStatus(in);
        System.out.println("目录 "+ in.getName()+" 创建成功. "+ fileStatus.getPath());
    }
    /**
     * 删除hdfs文件或者文件夹
     * @param path
     * @throws Exception
     */
    private void HDFSDelete(String path) throws Exception{
        FileSystem fs = FileSystem.get(configuration);
        Path in = new Path(path);
        if (!fs.exists(in)){
            System.out.println("路径不存在");
            System.exit(-1);
        }
        fs.delete(in,true);
        fs.close();
    }
    /**
     * 本地文件上传到hdfs
     * @param localfile
     * @param dst
     * @throws Exception
     */
    private void Loca2HDFSwithProgerss(String localfile,String dst) throws Exception{
        Configuration conf = new Configuration();
        Path p = new Path(dst);
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(localfile));
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream bos = fileSystem.create(p, new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in,bos,8192,true);
    }
    /**
     * 使用正则表达式过滤文件路径
     * @throws Exception
     */
    private void PathFilter() throws Exception{
        class Reg implements PathFilter{
            private String regex;
            public Reg() {
            }

            public Reg(String regex) {
                this.regex = regex;
            }
            public boolean accept(Path path) {
                // 返回不匹配的值
                return !path.toString().matches(regex);
            }
        }
        FileSystem fs = FileSystem.get(configuration);
        // 使用正则表达式过滤选择文件
        FileStatus[] fst = fs.globStatus(new Path("/*/*/*"),new Reg(".*/2017/12/31$"));

        for (FileStatus f : fst){
            System.out.println(f.toString());
        }
    }
    private void ListFileOnHDFS(String src) throws Exception{
        FileSystem fs = FileSystem.get(configuration);
        Path pathSrc = new Path(src);
        FileStatus[] fileStatus = fs.listStatus(pathSrc);
        for(FileStatus fs1 : fileStatus){
            System.out.println(fs1.getPath().getName());
        }
    }
    public void setConfiguration(){
        configuration = new Configuration();
    }
    public static void main(String[] args) throws Exception{
        App app = new App();
        //app.HDFS2Local("/1.mp4","D:\\1.mp4");
        app.setConfiguration();
        app.HDFS2Console("/a.txt");
        //app.ListFileOnHDFS("/");
      //  app.HDFS2Local("/1.mp4","D:\\1.mp4");
        //RandomAccessFile
    }
}