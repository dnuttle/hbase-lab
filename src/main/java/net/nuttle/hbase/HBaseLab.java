package net.nuttle.hbase;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Notes.
 * Lessons learned:
 * HTableInterface taken from HTablePool should call close when finished
 * HTableInterface must be disabled through HBaseAdmin before changes to structure, such as col families
 * HTableInterface must be enabled again through HBaseAdmin after changes to structure
 * HBaseAdmin should always have close method call
 * 
 * To investigate:
 * Difference between HTableInterface.flushCache and flushCommits
 * @author dan
 *
 */
public final class HBaseLab {

  private static final int MAX_POOL_SIZE = 1000;
  private static HTablePool pool = null;

  /**
   * Private constructor.
   */
  private HBaseLab() {
  }

  /**
   * getPool method.
   * @param conf is a Configuration instance
   * @return an HTablePool
   */
  public static HTablePool getPool(final Configuration conf) {
    if (pool == null) {
      pool = new HTablePool(conf, MAX_POOL_SIZE);
    }
    return pool;
  }

  /**
   * isTableAvailable method returns true if a specified table is available.
   * @param conf is an instance of Configuration
   * @param tableName is the name of an HBase table
   * @return true if the table exists
   * @throws IOException
   */
  public static boolean isTableAvailable(final Configuration conf, final String tableName) 
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      return admin.isTableAvailable(tableName);
    } finally {
      admin.close();
    }
  }

  /**
   * createTable creates an HBase table with a specified name and family.
   * @param conf is an instance of Configuration
   * @param tableName is the name of a table to be created
   * @param family is the name of a single family to be added to the table
   * @throws IOException
   * @throws HBaseException
   */
  public static void createTable(final Configuration conf, final String tableName, final String family) 
      throws IOException, HBaseException {
    String[] families = {family};
    createTable(conf, tableName, families);
  }

  /**
   * createTable creates an HBase table with a specified name and a specified array of families.
   * @param conf is an instance of Configuration
   * @param tableName is the name of the table to be created
   * @param families is an array of names of families to be added to the table
   * @throws IOException
   * @throws HBaseException
   */
  public static void createTable(final Configuration conf, final String tableName, final String[] families) 
      throws IOException, HBaseException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(tableName));
      for (String family : families) {
        HColumnDescriptor coldesc = new HColumnDescriptor(Bytes.toBytes(family));
        desc.addFamily(coldesc);
      }
      admin.createTable(desc);
      if (!admin.isTableAvailable(Bytes.toBytes(tableName))) {
        throw new HBaseException("Failed to create table " + tableName);
      }
    } finally {
      admin.close();
    }

  }

  /**
   * dropTable drops an HBase table.
   * @param conf is an instance of Configuration
   * @param tableName is the name of an HBase table
   * @throws IOException
   * @throws HBaseException
   */
  public static void dropTable(final Configuration conf, final String tableName) 
      throws IOException, HBaseException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      if (!admin.isTableAvailable(tableName)) {
        throw new HBaseException("Table " + tableName + " cannot be deleted, does not exist");
      }
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      if (admin.isTableAvailable(tableName)) {
        throw new HBaseException("Failed to delete table " + tableName);
      }
    } finally {
      admin.close();
    }
  }

  /**
   * Returns an HBase table.
   * @param conf is an instance of Configuration
   * @param tableName is the name of an HBase table
   * @return an HTableInterface is the table exists
   */
  public static HTableInterface getTable(final Configuration conf, final String tableName) {
    return getPool(conf).getTable(tableName);
  }

  /**
   * Adds a column family to an existing HBase table.
   * @param tableName is the name of a table
   * @param family is the name of a column family to be added
   * @throws IOException
   */
  public static void addFamily(final Configuration conf, final String tableName, final String family) 
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.disableTable(tableName);
      HColumnDescriptor colfam = new HColumnDescriptor(Bytes.toBytes(family));
      admin.addColumn(tableName, colfam);
      admin.enableTable(tableName);
    } finally {
      admin.close();
    }
  }

  /**
   * Drops a column family from a table.
   * @param tableName is the name of a table
   * @param family is the name of a column family
   * @throws IOException
   */
  public static void dropFamily(final Configuration conf, final String tableName, final String family) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      admin.disableTable(tableName);
      admin.deleteColumn(tableName, family);
      admin.enableTable(tableName);
    } finally {
      admin.close();
    }
  }

  /**
   * Returns true if a specified table contains a specified column family.
   * @param tableName is the name of a table
   * @param family is the name of a column family
   * @return true if the column family exists in the table
   * @throws IOException
   */
  public static boolean containsFamily(final Configuration conf, final String tableName, final String family) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(tableName));
      Collection<HColumnDescriptor> families = desc.getFamilies();
      for (HColumnDescriptor fam : families) {
        if (family.equals(fam.getNameAsString())) {
          return true;
        }
      }
      return false;
    } finally {
      admin.close();
    }

  }

  /**
   * Puts a value into an HBase table.
   * @param tableName is the name of a value.
   * @param family is a column family.
   * @param qualifier is a column name.
   * @param key is a rowkey.
   * @param value is the value to be stored.
   * @throws IOException
   */
  public static void put(final Configuration conf, final String tableName, final String family, final String qualifier, 
      final String key, final String value) throws IOException {
    HTableInterface table = getPool(conf).getTable(tableName);
    Put p = new Put(Bytes.toBytes(key));
    p.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    table.put(p);
    table.flushCommits();
    table.close();
  }

  /**
   * Returns a Result from a table for a given key and number of versions.
   * @param tableName is the name of a table
   * @param key is a rowkey
   * @param maxVersions is the maximum number of versions to return
   * @return a Result instance
   * @throws IOException
   */
  public static Result get(final Configuration conf, final String tableName, final String key, final int maxVersions) 
      throws IOException {
    HTableInterface table = getPool(conf).getTable(tableName);
    try {
      Get g = new Get(Bytes.toBytes(key));
      g.setMaxVersions(maxVersions);
      Result r = table.get(g);
      return r;
    } finally {
      table.close();
    }
  }

  /**
   * Returns a Result containing the latest value for a given key in a table.
   * @param tableName is the name of a table
   * @param key is a rowkey
   * @return a Result instance
   * @throws IOException
   */
  public static Result getLatest(final Configuration conf, final String tableName, final String key) throws IOException {
    return get(conf, tableName, key, 1);
  }

  /**
   * Deletes a row from a table.
   * @param tableName is the name of an HBase table
   * @param key is a rowkey
   * @throws IOException
   */
  public static void delete(final Configuration conf, final String tableName, final String key) 
      throws IOException {
    HTableInterface table = getPool(conf).getTable(tableName);
    try {
      Delete d = new Delete(Bytes.toBytes(key));
      table.delete(d);
      table.flushCommits();
    } finally {
      table.close();
    }
  }

  /**
   * increment increases an HBase cell by a specified value.
   * @param tableName is the name of an HBase table
   * @param key is a rowkey
   * @param family is an HBase column family
   * @param qualifier is an HBase column
   * @param value is a long value to be added to the current value
   * @throws IOException
   */
  public static void increment(final Configuration conf, final String tableName, final String key, final String family, 
    final String qualifier, final long value) 
    throws IOException {
    HTableInterface table = getPool(conf).getTable(tableName);
    try {
      table.incrementColumnValue(Bytes.toBytes(key), Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
    } finally {
      table.close();
    }
  }

  /**
   * scan method returns a ResultScanner for a specified table and filter.
   * @param tableName is the name of an HBase table
   * @param filter is an HBase filter
   * @return a ResultScanner for the table and filter
   * @throws IOException
   */
  public static ResultScanner scan(final String tableName, final Filter filter, int caching) 
      throws IOException {
    HTableInterface table = pool.getTable(tableName);
    try {
      Scan scan = new Scan();
      scan.setFilter(filter);
      scan.setCaching(caching);
      ResultScanner scanner = table.getScanner(scan);
      
      return scanner;
    } finally {
      table.close();
    }
  }
  
}
