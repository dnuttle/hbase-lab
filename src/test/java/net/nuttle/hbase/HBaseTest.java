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
  public final void testGetTable() {
    testHBase();
    try {
      if (HBaseLab.isTableAvailable(conf, TABLE_NAME)) {
        fail("Table exists, not expected");
      }
      createTable(TABLE_NAME, FAMILY);
      if (!isTableAvailable(TABLE_NAME)) {
        fail("Table does not exist");
      }
      HTableInterface table = getTable(TABLE_NAME);
      assertNotNull("HTableInterface object is null", table);
      assertEquals(TABLE_NAME, Bytes.toString(table.getTableName()));
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * Test creating a table.
   */
  @Test
  public final void testCreateTable() {
    testHBase();
    try {
      if (isTableAvailable(TABLE_NAME)) {
        fail("Table exists, not expected");
      }
      HBaseLab.createTable(conf, TABLE_NAME, FAMILY);
      assert (isTableAvailable(TABLE_NAME));
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getName());
    }
  }

  /**
   * Test dropping a table.
   */
  @Test
  public final void testDropTable() {
    testHBase();
    try {
      if (isTableAvailable(TABLE_NAME)) {
        fail("Table exists, not expected");
      }
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.dropTable(conf, TABLE_NAME);
      assertTrue(!isTableAvailable(TABLE_NAME));
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getName());
    }
  }

  /**
   * Test writing a cell to a table and reading it back.
   */
  @Test
  public final void testWriteTable() {
    testHBase();
    try {
      String value = "123456";
      if (isTableAvailable(TABLE_NAME)) {
        fail("Table " + TABLE_NAME + " exists, not expected");
      }
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER, KEY, "ZZZ");
      Thread.sleep(SLEEP);
      HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER, KEY, value);

      Result r = HBaseLab.getLatest(TABLE_NAME, KEY);
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
      HBaseLab.delete(TABLE_NAME, KEY);
      r = HBaseLab.getLatest(TABLE_NAME, KEY);
      values = r.getColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER));
      assertEquals(0, values.size());
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * The increment method increases a cell value by a specified number.
   */
  @Test
  public final void testIncrementTable() {
    testHBase();
    try {
      long incrementCount = INCREMENT_COUNT;
      if (isTableAvailable(TABLE_NAME)) {
        fail("Table exists, not expected");
      }
      createTable(TABLE_NAME, FAMILY);
      HTableInterface table = getTable(TABLE_NAME);
      for (int i = 0; i < incrementCount; i++) {
        HBaseLab.increment(TABLE_NAME, KEY, FAMILY, QUALIFIER, 1L);
        //table.incrementColumnValue(Bytes.toBytes(KEY), Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), 1L);
      }
      Get g = new Get(Bytes.toBytes(KEY));
      Result result = table.get(g);
      List<KeyValue> values = result.getColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER));
      assertEquals(1, values.size());
      KeyValue kv = values.get(0);
      assertEquals(incrementCount, Bytes.toLong(kv.getValue()));
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * The RowFilter filters records by row key.
   */
  @Test
  public final void testRowFilter() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER, "row-" + i, "val-" + i);
      }
      RowFilter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, 
        new BinaryComparator(Bytes.toBytes("row-20")));
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r : scanner) {
        count++;
      }
      assertEquals(FILTERED_ROW_COUNT, count);
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }
  
  /**
   * The FamilyFilter filters records by column family name. Those with the family name
   * are returned when using CompareOp.EQUAL.
   */
  @Test
  public final void testFamilyFilter() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.addFamily(TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(TABLE_NAME, FAMILY2, QUALIFIER2, "row-2-" + i, "val-" + i);
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
      for (Result r: scanner) {
        count++;
      }
      assertEquals(ROW_COUNT * 2, count);
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }
  
  /**
   * The QualifierFilter filters records by qualifier name; those with the
   * family/qualifier are returned when using CompareOp.EQUAL.
   */
  @Test
  public final void testQualifierFilter() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.addFamily(TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER2, "row-2-" + i, "val-" + i);
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
      for (Result r: scanner) {
        count++;
      }
      assertEquals(ROW_COUNT * 2, count);
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * The ValueFilter filters records by value.
   */
  @Test
  public final void testValueFilter() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.addFamily(TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER2, "row-2-" + i, "val-" + i);
      }
      //Get all rows with family/column FAMILY2/QUALIFIER2
      ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
        new SubstringComparator("val-0"));
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r: scanner) {
        count++;
        assertTrue(Bytes.toString(r.value()).contains("val-0"));
      }
      assertEquals(2, count);
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
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
  public final void testSingleColumnValueFilter() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.addFamily(TABLE_NAME, FAMILY2);
      for (int i = 0; i < ROW_COUNT; i++) {
        HBaseLab.put(TABLE_NAME,  FAMILY,  QUALIFIER, "row-" + i, "val-" + i);
        HBaseLab.put(TABLE_NAME, FAMILY2, QUALIFIER2, "row-2-" + i, "val-" + i);
      }
      HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER2, "row-0", "val-0");
      //Get all rows with family/column FAMILY2/QUALIFIER2
      SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), 
        CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val-0"));
      //Reverse default functionality; return only those rows that do have the specified column.
      filter.setFilterIfMissing(true);
      ResultScanner scanner = HBaseLab.scan(TABLE_NAME, filter);
      int count = 0;
      for (Result r: scanner) {
        count++;
        KeyValue[] kvs = r.raw();
        for (KeyValue kv : kvs) {
          LOG.info(Bytes.toString(kv.getFamily()));
          LOG.info(Bytes.toString(kv.getQualifier()));
          LOG.info(Bytes.toString(kv.getValue()));
          LOG.info(Bytes.toString(kv.getRow()));
          //assertEquals(FAMILY, Bytes.toString(kv.getFamily()));
          //assertEquals(QUALIFIER, Bytes.toString(kv.getQualifier()));
        }
        LOG.info(Bytes.toString(r.value()));
        LOG.info(Bytes.toString(r.getRow()));
        //assertEquals("val-0", Bytes.toString(r.value()));
      }
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * Test adding a column family to an existing table.
   */
  @Test
  public final void testAddFamily() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.addFamily(TABLE_NAME, FAMILY2);
      HBaseLab.put(TABLE_NAME, FAMILY2, QUALIFIER2, KEY, "123456");
      Result r = HBaseLab.getLatest(TABLE_NAME, KEY);
      r.getColumnLatest(Bytes.toBytes(FAMILY2), Bytes.toBytes(QUALIFIER2));
      assertEquals("123456", Bytes.toString(r.value()));
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * Test dropping a column family from an existing table.
   */
  @Test
  public final void testDropFamily() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      HBaseLab.addFamily(TABLE_NAME, FAMILY2);
      HBaseLab.put(TABLE_NAME, FAMILY, QUALIFIER, KEY, "XYZ");
      HBaseLab.put(TABLE_NAME, FAMILY2, QUALIFIER2, KEY, "123456");
      HBaseLab.dropFamily(TABLE_NAME, FAMILY);
      Result r = HBaseLab.getLatest(TABLE_NAME, KEY);
      assertTrue(r.containsColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(QUALIFIER2)));
      assertTrue(!r.containsColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER)));
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
    }
  }

  /**
   * testContainsFamily method.
   */
  @Test
  public final void testContainsFamily() {
    testHBase();
    try {
      createTable(TABLE_NAME, FAMILY);
      assertTrue(HBaseLab.containsFamily(TABLE_NAME, FAMILY));
      dropTable(TABLE_NAME);
    } catch (Exception e) {
      fail("Unexpected " + e.getClass().getSimpleName());
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
    HBaseLab.dropTable(conf, tableName);
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
