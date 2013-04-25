import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;

public class hdfs_example01 {

    /**
     *      @param args
     */
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.print("Need two arguments\n");
            System.out.flush();
            return;
        }
        System.out.print("Source File: "+args[0]+"\n");
        System.out.print("Destination File: "+args[1]+"\n");
        System.out.flush();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            DistributedFileSystem hdfs = (DistributedFileSystem)fs;

            // Copy a file from the local file system to HDFS
            Path srcPath = new Path(args[0]);
            Path dstPath = new Path(args[1]);
            hdfs.copyFromLocalFile(srcPath, dstPath);
        } catch (Exception e) {
            // handle exception
            e.printStackTrace();
        }
    }
}

