package com.dkl.blog.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
 *Java API 连接 HBASE
 */
public class JavaHbaseExample {
    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {//如果表已存在
            if (admin.isTableEnabled(table.getTableName())) {//如果表状态为Enabled
                admin.disableTable(table.getTableName());
            }
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    public static void modifySchema(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            HTableDescriptor table = admin.getTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            //admin.addColumn(tableName, newColumn); //官方文档代码，这里我理解的是应该要添加列簇，但是该方法不生效，
            // 导致抛出异常org.apache.hadoop.hbase.InvalidFamilyOperationException:
            // Family 'DEFAULT_COLUMN_FAMILY' is the only column family in the table, so it cannot be deleted,
            // 用我下面这行代码
            table.addFamily(newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);


            // Disable an existing table
            admin.disableTable(tableName);
            // Delete an existing column family
            admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        }
    }

    public static void main(String... args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.44.128");  //hbase 服务地址,如果在hbase-site.xml里有配置，可以不用这行代码
//        config.set("hbase.zookeeper.property.clientPort","2181"); //默认2181端口

//        System.out.println(System.getenv("HBASE_CONF_DIR"));

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
//        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml")); //官方文档代码，需要配置环境变量
        config.addResource(new Path("D:\\data\\conf\\hbase", "hbase-site.xml"));
//        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));//官方文档代码，需要配置环境变量
        config.addResource(new Path("D:\\data\\conf\\hadoop", "core-site.xml"));
        createSchemaTables(config);
        modifySchema(config);
    }
}
