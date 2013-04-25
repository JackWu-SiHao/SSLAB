import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;

public class hdfs_example03 {

    /**
     *      @param args
     */
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.print("Need two arguments\n");
            System.out.flush();
            return;
        }
        System.out.print("Old Filename: "+args[0]+"\n");
        System.out.print("New Filename: "+args[1]+"\n");
        System.out.flush();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
			DistributedFileSystem hdfs = (DistributedFileSystem)fs;

            // Rename a file in HDFS
            Path fromPath = new Path(args[0]);
            Path toPath = new Path(args[1]);
            boolean isRenamed = hdfs.rename(fromPath, toPath);
            if (isRenamed) {
                System.out.print("Rename file successfully.\n");
                System.out.flush();
            } else {
                System.out.print("File not renamed!\n");
                System.out.flush();
            }
        } catch (Exception e) {
            // handle exception
            e.printStackTrace();
        }
    }
}