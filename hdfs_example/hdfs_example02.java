import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;

public class hdfs_example02 {

    /**
     *      @param args
     */
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.print("Need two arguments\n");
            System.out.flush();
            return;
        }
        System.out.print("Creat File: "+args[0]+"\n");
        System.out.print("Content: "+args[1]+"\n");
        System.out.flush();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
			DistributedFileSystem hdfs = (DistributedFileSystem)fs;

            // Create a file in HDFS
            Path filePath = new Path(args[0]);
            FSDataOutputStream outputStream = hdfs.create(filePath);
            byte[] buff = args[1].getBytes();
            outputStream.write(buff, 0, buff.length);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
}