import java.util.Date;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;

public class hdfs_example06 {

    /**
     *      @param args
     */
    public static void main(String[] args) {

        System.out.print("Path: "+args[0]+"\n");
        System.out.flush();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            DistributedFileSystem hdfs = (DistributedFileSystem)fs;
            Path targetPath = new Path(args[0]);

            // Check if a file or directory exists in HDFS
            if (!hdfs.exists(targetPath)) {
			  System.out.print("File/Directory not found!\n");
                System.out.flush();
                return;
            }

            // Check if it is a file
            FileStatus fileStatus = hdfs.getFileStatus(targetPath);
            if (fileStatus.isDir()) {
                System.out.print("Not a file!\n");
                System.out.flush();
                return;
            }

            // Get the locations of blocks of a file in HDFS
            BlockLocation[] blkLocations =
                hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            int blkCount = blkLocations.length;
            for (int i=0; i < blkCount; i++) {
                String[] hosts = blkLocations[i].getHosts();
                System.out.print("Block "+i+": ");
                for (int j=0; j < hosts.length; j++) {
                    System.out.print(hosts[j]+" ");
                }
				System.out.print("\n");
            }
        } catch (Exception e) {
            // handle exception
            e.printStackTrace();
        }
    }
}
