package net.nuttle.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * HBaseTest.
 */
public class HBaseTest {

  private static final Logger LOG = Logger.getLogger(HBaseTest.class);
  private static final String TABLE_NAME = "test";
  private static final String FAMILY = "colfam1";
  private static final String QUALIFIER = "qual1";
  private static final String FAMILY2 = "colfam2";
  private static final String QUALIFIER2 = "qual2";
  private static final String KEY = "KEY1000";
  private static final int SLEEP = 500;
  private static final int ROW_COUNT = 30;
  private static final int FILTERED_ROW_COUNT = 14;
  private static final int INCREMENT_COUNT = 1237;
  
  private static boolean hBaseAvailable = true;
  private static Configuration conf;

  /**
   * setUp method.
   */
  @BeforeClass
  public static void setUp() {
    try {
      conf = HBaseConfiguration.create();
      conf.set("hbase.tmp.dir", "./tmp");
      LOG.info("hbase.rootdir: " + conf.get("hbase.rootdir"));
      if (isTableAvailable(TABLE_NAME)) {
        dropTable(TABLE_NAME);
      }
    } catch (Exception e) {
      hBaseAvailable = false;
    }
  }

  /**
   * Test getting a table.
   */
  @Test
  public final void testGetTable() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      if (!isTableAvailable(TABLE_NAME)) {
        fail("Table does not exist");
      }
      HTableInterface table = getTable(TABLE_NAME);
      assertNotNull("HTableInterface object is null", table);
      assertEquals(TABLE_NAME, Bytes.toString(table.getTableName()));
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * Test creating a table.
   */
  @Test
  public final void testCreateTable() throws IOException, HBaseException {
    testHBase();
    HBaseLab.createTable(conf, TABLE_NAME, FAMILY);
    try {
      assert (isTableAvailable(TABLE_NAME));
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * Test dropping a table.
   */
  @Test
  public final void testDropTable() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    HBaseLab.dropTable(conf, TABLE_NAME);
    assertTrue(!isTableAvailable(TABLE_NAME));
  }

  /**
   * Test writing a cell to a table and reading it back.
   */
  @Test
  public final void testWriteTable() throws IOException, HBaseException, InterruptedException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      String value = "123456";
      HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER, KEY, "ZZZ");
      Thread.sleep(SLEEP);
      HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER, KEY, value);

      Result r = HBaseLab.getLatest(conf, TABLE_NAME, KEY);
      //The Result.value() method returns the value of the newest cell 
      //in the first column found
      assertEquals(value, Bytes.toString(r.value()));
      //The Result.getValue() method returns the value of the newest cell in the  
      //specified family/column
      assertEquals(Bytes.toString(r.value()), Bytes.toString(r.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER))));
      //The Result.getColumn() method returns a List of KeyValue instances for a specified 
      //family/column
      //This may return all the values that have been set.
      List<KeyValue> values = r.getColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER));
      //The Result.getColumnLatest() returns only the latest KeyValue for specified family/column,
      //but not that the Get must set maxSize to a value > 1
      assertEquals(1, values.size());
      KeyValue kv = values.get(0);
      assertEquals(KEY, Bytes.toString(kv.getRow()));
      assertEquals(value, Bytes.toString(kv.getValue()));
      HBaseLab.delete(conf, TABLE_NAME, KEY);
      r = HBaseLab.getLatest(conf, TABLE_NAME, KEY);
      values = r.getColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER));
      assertEquals(0, values.size());
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * The increment method increases a cell value by a specified number.
   */
  @Test
  public final void testIncrementTable() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      long incrementCount = INCREMENT_COUNT;
      HTableInterface table = getTable(TABLE_NAME);
      for (int i = 0; i < incrementCount; i++) {
        HBaseLab.increment(conf, TABLE_NAME, KEY, FAMILY, QUALIFIER, 1L);
        //table.incrementColumnValue(Bytes.toBytes(KEY), Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), 1L);
      }
      Get g = new Get(Bytes.toBytes(KEY));
      Result result = table.get(g);
      List<KeyValue> values = result.getColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER));
      assertEquals(1, values.size());
      KeyValue kv = values.get(0);
      assertEquals(incrementCount, Bytes.toLong(kv.getValue()));
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * The RowFilter filters records by row key.
   */
  @Test
  public final void testRowFilter() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER, "row-" + i, "val-" + i);
      }
      RowFilter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, 
        new BinaryComparator(Bytes.toBytes("row-20")));
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (@SuppressWarnings("unused") Result r : scanner) {
        count++;
      }
      assertEquals(FILTERED_ROW_COUNT, count);
    } finally {
      dropTable(TABLE_NAME);
    }
  }
  
  /**
   * The FamilyFilter filters records by column family name. Those with the family name
   * are returned when using CompareOp.EQUAL.
   */
  @Test
  public final void testFamilyFilter() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      HBaseLab.addFamily(conf, TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(conf, TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(conf, TABLE_NAME, FAMILY2, QUALIFIER2, "row-2-" + i, "val-" + i);
      }
      //Get all rows with family/column FAMILY2/QUALIFIER2
      FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(FAMILY2)));
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r: scanner) {
        count++;
        assertTrue(r.containsColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(QUALIFIER2)));
      }
      assertEquals(ROW_COUNT, count);
      //Now pass in a null for filter, so that all rows are returned;
      scanner = HBaseLab.scan(TABLE_NAME, null);
      count = 0;
      for (@SuppressWarnings("unused") Result r: scanner) {
        count++;
      }
      assertEquals(ROW_COUNT * 2, count);
    } finally {
      dropTable(TABLE_NAME);
    }
  }
  
  /**
   * The QualifierFilter filters records by qualifier name; those with the
   * family/qualifier are returned when using CompareOp.EQUAL.
   */
  @Test
  public final void testQualifierFilter() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      HBaseLab.addFamily(conf, TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(conf, TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER2, "row-2-" + i, "val-" + i);
      }
      //Get all rows that have column QUALIFIER2
      QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes(QUALIFIER2)));
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r: scanner) {
        count++;
        assertTrue(r.containsColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER2)));
      }
      assertEquals(ROW_COUNT, count);
      //Now pass in a null for filter, so that all rows are returned;
      scanner = HBaseLab.scan(TABLE_NAME, null);
      count = 0;
      for (@SuppressWarnings("unused") Result r: scanner) {
        count++;
      }
      assertEquals(ROW_COUNT * 2, count);
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * The ValueFilter filters records by value.
   */
  @Test
  public final void testValueFilter() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      HBaseLab.addFamily(conf, TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(conf, TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER2, "row-2-" + i, "val-" + i);
      }
      //Get all rows with value "val-0"
      ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
        new SubstringComparator("val-0"));
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r: scanner) {
        count++;
        assertTrue(Bytes.toString(r.value()).contains("val-0"));
      }
      assertEquals(2, count);
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * The SingleColumnValueFilter filters records by value, based on a single column.
   * By default, rows are returned when either of the following is true:
   * 1. The family/qualifier exists, and the value is equal to the value passed in.
   * 2. The family/qualifier does not exist.
   * This is somewhat counter-intuitive.  The idea seems to be that, a row is only excluded if
   * a cell for a specified family/qualifier has the wrong value.
   * A use case might be "Return all records except those whose USER field is not equal to SMITH."
   * The setFilterIfMissing method, if passed true,
   * will mean that records are only returned if the family/qualifier exists, and its cell 
   * has the specified value.
   * Still more to understand!  I wrote two records, to two columns in the same family,
   * with same rowkey and value.  I got a single Result, with two KeyValue instances, one for each family/qualifier.
   */
  @Test
  public final void testSingleColumnValueFilter() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      HBaseLab.addFamily(conf, TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(conf, TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(conf, TABLE_NAME, FAMILY2, QUALIFIER2, "row-2-" + i, "val-" + i);
      }
      HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER2, "row-0", "val-0");
      //Get all rows with family/column FAMILY2/QUALIFIER2
      SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), 
        CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val-0"));
      //Reverse default functionality; return only those rows that do have the specified column.
      filter.setFilterIfMissing(true);
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r: scanner) {
        KeyValue[] kvs = r.raw();
        for (KeyValue kv : kvs) {
          LOG.info(Bytes.toString(kv.getFamily()));
          LOG.info(Bytes.toString(kv.getQualifier()));
          LOG.info(Bytes.toString(kv.getValue()));
          LOG.info(Bytes.toString(kv.getRow()));  
          assertEquals(FAMILY, Bytes.toString(kv.getFamily()));
          //The following is invalid, if we put another instance of "val-0" in using QUALIFIER2
          //I don't remember what I was trying to accomplish by doing that.
          //assertEquals(QUALIFIER, Bytes.toString(kv.getQualifier()));
          count++;
        }
        assertEquals(2, count);
        LOG.info(Bytes.toString(r.value()));
        LOG.info(Bytes.toString(r.getRow()));
        assertEquals("val-0", Bytes.toString(r.value()));
      }
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * Test adding a column family to an existing table.
   */
  @Test
  public final void testAddFamily() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      HBaseLab.addFamily(conf, TABLE_NAME, FAMILY2);
      HBaseLab.put(conf, TABLE_NAME, FAMILY2, QUALIFIER2, KEY, "123456");
      Result r = HBaseLab.getLatest(conf, TABLE_NAME, KEY);
      r.getColumnLatest(Bytes.toBytes(FAMILY2), Bytes.toBytes(QUALIFIER2));
      assertEquals("123456", Bytes.toString(r.value()));
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * Test dropping a column family from an existing table.
   */
  @Test
  public final void testDropFamily() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      HBaseLab.addFamily(conf, TABLE_NAME, FAMILY2);
      HBaseLab.put(conf, TABLE_NAME, FAMILY, QUALIFIER, KEY, "XYZ");
      HBaseLab.put(conf, TABLE_NAME, FAMILY2, QUALIFIER2, KEY, "123456");
      HBaseLab.dropFamily(conf, TABLE_NAME, FAMILY);
      Result r = HBaseLab.getLatest(conf, TABLE_NAME, KEY);
      assertTrue(r.containsColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(QUALIFIER2)));
      assertTrue(!r.containsColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER)));
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  /**
   * testContainsFamily method.
   */
  @Test
  public final void testContainsFamily() throws IOException, HBaseException {
    testHBase();
    createTable(TABLE_NAME, FAMILY);
    try {
      assertTrue(HBaseLab.containsFamily(conf, TABLE_NAME, FAMILY));
    } finally {
      dropTable(TABLE_NAME);
    }
  }

  // UTILITY METHODS
  /**
   * createTable method.
   * @param tableName is the name of an HBase table
   * @param family is the name of a column family
   * @throws IOException
   * @throws HBaseException
   */
  private static void createTable(final String tableName, final String family) 
      throws IOException, HBaseException {
    if (HBaseLab.isTableAvailable(conf, tableName)) {
      throw new HBaseException("Table already exists");
    }
    HBaseLab.createTable(conf, tableName, family);
  }

  /**
   * getTable method.
   * @param tableName is the name of an HBase table
   * @return HTableInterface
   */
  private static HTableInterface getTable(final String tableName) {
    HTablePool pool = HBaseLab.getPool(conf);
    return pool.getTable(tableName);
  }

  /**
   * dropTable method.
   * @param tableName is the name of an HBase table
   * @throws HBaseException
   * @throws IOException
   */
  private static void dropTable(final String tableName) 
      throws HBaseException, IOException {
    if(isTableAvailable(tableName)) {
      HBaseLab.dropTable(conf, tableName);
    }
  }

  /**
   * isTableAvailable method returns true if an HBase table exists.
   * @param tableName is the name of an HBase table
   * @return true if the table exists
   * @throws IOException
   */
  private static boolean isTableAvailable(final String tableName) 
      throws IOException {
    return HBaseLab.isTableAvailable(conf, tableName);
  }

  /**
   * testHBase returns true if HBase was available when the class was loaded.
   */
  private static void testHBase() {
    assertTrue("HBase not available", hBaseAvailable);
  }
}
