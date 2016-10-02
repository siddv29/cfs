# cfs - Cassandra Fast full table Scan
You can use this utility to scan your table in cassandra database at a very very fast speed.<br><br>
All you need to do is provide <br>
<ul>
<li>Table identifier</li>
<li>Cluster node IP</li>
<li>Queue (LinkedBlockingQueue)
<li>Configurable options ( <i>values in brackets are default values</i> )</li>
<ul>
<li>Username ( <i>nill</i> )</li>
<li>Password ( <i>nill</i> )</li>
<li>Consistency Level ( <i>LOCAL_ONE</i> )</li>
<li>Number of threads ( <i>16</i> )</li>
<li>DC Aware Option ( <i>all nodes considered</i> )</li>
<li>ColumnNames to be dumped ( <i>*</i> )</li>
<li>Fetch Size per page ( <i>5000</i> )</li>
</ul>
</ul>
##How fast are we talking about?

##How to use CFS?
###Download
You can download the jar from <a href="https://drive.google.com/file/d/0Bx4phBKd267eNXBncEN2aVlLREk/view?usp=sharing">here</a>.<br>
Or, you may download the source file and run ```mvn clean install``` to use ```cfs-1.0-SNAPSHOT-full.jar```
###Sample Program
```
package com.cassandra.utility.trial;

import com.cassandra.utility.method1.CassandraFastFullTableScan;
import com.cassandra.utility.method1.Options;
import com.cassandra.utility.method1.RowTerminal;
import com.datastax.driver.core.Row;
import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String... args) throws Exception{
        LinkedBlockingQueue<Row> queue =new LinkedBlockingQueue<>();
        
        CassandraFastFullTableScan cfs = 
                new CassandraFastFullTableScan("mykeyspace.table_name",
                        "10.41.55.111",queue,
                        new Options().setUsername("cassandra").setPassword("cassandra"),
                        new PrintStream(new File("/tmp/cfs_round_1.log")));
                        
        CountDownLatch countDownLatch = cfs.start();
        
        new NotifyWhenCFSFinished(countDownLatch).start();
        
        Row row;
        int counter=0;
        while(! ((row = queue.take()) instanceof RowTerminal)){
            System.out.println(++counter+":"+row);
            /*
              you can use row.getString("column1") and so on
            */
        }
    }

    static class NotifyWhenCFSFinished extends Thread{
        CountDownLatch latch;

        public NotifyWhenCFSFinished(CountDownLatch latch) {
            this.latch = latch;
        }

        public void run(){
            System.out.println("Waiting for CFS to complete");
            try{
                latch.await();
            }catch (Exception e1){
                //ignore
            }
            System.out.println("CFS completed");
        }

    }

}
```
###Explanation
####Traditional Cassandra Scan
![alt tag](https://github.com/siddv29/cfs/blob/master/images/TraditionalCassandrScan.png)
####CFS
![alt tag](https://github.com/siddv29/cfs/blob/master/images/CFS.png)
##Contributing
Hi, if it interests you, 
<br>kindly pull the code, and go through it.
<br>If you would like to build it more, drop a mail at sidd.verma29.lists@gmail.com
<br>This was prepared ASAP. Would love to collaborate on it, to increase funcationality.


