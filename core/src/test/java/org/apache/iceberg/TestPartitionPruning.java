/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestPartitionPruning {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
//        new Object[] { 2 },
    };
  }

  public final int formatVersion;

  public TestPartitionPruning(int formatVersion) {
    this.formatVersion = formatVersion;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Schema schema;
  private File tableDir = null;
  private Map<Type.PrimitiveType, ByteBuffer> lbs;
  private Map<Type.PrimitiveType, ByteBuffer> ubs;

  private final Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
  private final Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
  private Metrics columnMetrics;

  private final int tableWidth = 100;
  private final int numStores = 100;
  private final int numFiles = 1000;
  private final int numInitialDays = 180;
  private final long numRowsPerFile = 1_000_000L;
  private final long timeLimit = 10l;

  private final boolean deleteDataFiles = true;

  private int numOfFilesExpected = 0;

  @Before
  public void setupTableDir() throws IOException {
    this.tableDir = temp.newFolder();
    this.lbs = getLowerBounds();
    this.ubs = getUpperBounds();
    this.schema = getSchema(tableWidth);
    this.columnMetrics = new Metrics(numRowsPerFile, Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap(), lowerBounds, upperBounds);
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  int getPercent(int width, int percent) {
    return (width * percent) / 100;
  }

  List<Types.NestedField> getColumnsOfType(int numCols, int startId, Type.PrimitiveType type) {
    List<Types.NestedField> columns = new ArrayList<>(numCols);

    ByteBuffer lowerBound = lbs.get(type);
    ByteBuffer upperBound = ubs.get(type);
    int id = startId;
    for(int i = 0; i < numCols; i++, id++) {
      columns.add(optional(id, "col" + id, type));
      lowerBounds.put(id, lowerBound);
      upperBounds.put(id, upperBound);
    }

    return columns;
  }

  Schema getSchema(int width) {
    int id = 1;
    List<Types.NestedField> columns = new ArrayList<>(width + 2);
    columns.add(required(id++, "date", Types.IntegerType.get()));
    columns.add(required(id++, "storeId", Types.IntegerType.get()));

    int numCols = 0;
    // string would be 25% of columns
    int numString = getPercent(width, 25);
    columns.addAll(getColumnsOfType(numString, id + numCols, Types.StringType.get()));
    numCols += numString;

    // date would be 5% of columns   - 30%
    int numDate = getPercent(width, 5);
    columns.addAll(getColumnsOfType(numDate, id + numCols, Types.DateType.get()));
    numCols += numDate;

    // timestamp would be 10% of columns  - 40%
    int numTs = getPercent(width, 10);
    columns.addAll(getColumnsOfType(numTs, id + numCols, Types.TimestampType.withoutZone()));
    numCols += numTs;

    // decimal would be 10% of columns  - 50%
    int numDecimal = getPercent(width, 10);
    columns.addAll(getColumnsOfType(numDecimal, id + numCols, Types.DecimalType.of(29, 10)));
    numCols += numDecimal;

    // float would be 5% of columns  - 55%
    int numFloat = getPercent(width, 5);
    columns.addAll(getColumnsOfType(numFloat, id + numCols, Types.FloatType.get()));
    numCols += numFloat;

    // double would be 5% of columns - 60%
    int numDouble = getPercent(width, 5);
    columns.addAll(getColumnsOfType(numDouble, id + numCols, Types.DoubleType.get()));
    numCols += numDouble;

    // long would be 10% of columns  - 70%
    int numLong = getPercent(width, 10);
    columns.addAll(getColumnsOfType(numLong, id + numCols, Types.LongType.get()));
    numCols += numLong;

    // int would be the rest - 100%
    int numInt = width - numCols;
    if (numInt <= 0) {
      numInt = 1;
    }
    columns.addAll(getColumnsOfType(numInt, id + numCols, Types.IntegerType.get()));
    numCols += numInt;

    System.out.println("Schema width " + numCols);
    return new Schema(columns);
  }

  Map<Type.PrimitiveType, ByteBuffer> getLowerBounds() {
    Map<Type.PrimitiveType, ByteBuffer> bounds = new HashMap<>();
    bounds.put(Types.IntegerType.get(), Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    bounds.put(Types.LongType.get(), Conversions.toByteBuffer(Types.LongType.get(), 10L));
    bounds.put(Types.FloatType.get(), Conversions.toByteBuffer(Types.FloatType.get(), -4.5f));
    bounds.put(Types.DoubleType.get(), Conversions.toByteBuffer(Types.DoubleType.get(), -8.2d));
    // days since 1970-01-01
    bounds.put(Types.DateType.get(), Conversions.toByteBuffer(Types.DateType.get(), 365 * 30));
    bounds.put(Types.TimeType.get(), Conversions.toByteBuffer(Types.TimeType.get(), 3L * 60L * 60L * 1000L * 1000L * 1000L));
    bounds.put(Types.TimestampType.withoutZone(), Conversions.toByteBuffer(Types.TimestampType.withoutZone(), 365L * 30L * 3L * 60L * 60L * 1000L * 1000L * 1000L));
    bounds.put(Types.StringType.get(), Conversions.toByteBuffer(Types.StringType.get(), "ABC"));
    bounds.put(Types.DecimalType.of(29, 10), Conversions.toByteBuffer(Types.DecimalType.of(29, 10), new BigDecimal(1234567890123456789.0123456789)));

    return bounds;
  }

  Map<Type.PrimitiveType, ByteBuffer> getUpperBounds() {
    Map<Type.PrimitiveType, ByteBuffer> bounds = new HashMap<>();
    bounds.put(Types.IntegerType.get(), Conversions.toByteBuffer(Types.IntegerType.get(), 100));
    bounds.put(Types.LongType.get(), Conversions.toByteBuffer(Types.LongType.get(), 100000L));
    bounds.put(Types.FloatType.get(), Conversions.toByteBuffer(Types.FloatType.get(), 44445.5f));
    bounds.put(Types.DoubleType.get(), Conversions.toByteBuffer(Types.DoubleType.get(), 8089234.2d));
    // days since 1970-01-01
    bounds.put(Types.DateType.get(), Conversions.toByteBuffer(Types.DateType.get(), 365 * 50));
    bounds.put(Types.TimeType.get(), Conversions.toByteBuffer(Types.TimeType.get(), 12L * 60L * 60L * 1000L * 1000L * 1000L));
    bounds.put(Types.TimestampType.withoutZone(), Conversions.toByteBuffer(Types.TimestampType.withoutZone(), 365L * 50L * 123L * 60L * 60L * 1000L * 1000L * 1000L));
    bounds.put(Types.StringType.get(), Conversions.toByteBuffer(Types.StringType.get(), "ZZZZ This is a long string"));
    bounds.put(Types.DecimalType.of(29, 10), Conversions.toByteBuffer(Types.DecimalType.of(29, 10), new BigDecimal(9999999999999999999.9999999999)));

    return bounds;
  }

  @Test
  public void testFilterFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("date").identity("storeId").build();
    TestTables.TestTable table = TestTables.create(tableDir, "dateAndStoreIdTable", schema, spec, formatVersion);

    // Generate 6 months worth of data
    int dayNum = numInitialDays;
    createFilesForDate(table, numInitialDays, numStores, numFiles);

    System.out.println("Loaded initial data, numInitialDays=" + numInitialDays + ", numStores=" + numStores + ", numFiles=" + numFiles);
    //System.out.println("D, TT, MR, BR, E, FR");
    runQueries(table, dayNum);
    summarizeTable(table);

    /*
    // Add and delete files for another month and run the same queries
    for(int i = 0; i < 10; i++) {
      addAndDeleteFilesForDate(table, ++dayNum, numStores, numFiles, numInitialDays);
      summarizeTable(table);
    }
     */
  }

  private void summarizeTable(Table table) {
    Snapshot snapshot = table.currentSnapshot();

    TestTables.resetCounters();

    int numFilesAdded = 0;
    int numFilesDeleted = 0;
    int numExistingFiles = 0;

    long rowsAdded = 0;
    long rowsDeleted = 0;
    long rowsExisting = 0;

    Stopwatch stopwatch = Stopwatch.createStarted();
    for(ManifestFile manifestFile : snapshot.allManifests()) {
      int addedFiles = manifestFile.addedFilesCount();
      int deletedFiles = manifestFile.deletedFilesCount();
      int existingFiles = manifestFile.existingFilesCount();

      numFilesAdded += manifestFile.addedFilesCount();
      numFilesDeleted += manifestFile.deletedFilesCount();
      numExistingFiles += manifestFile.existingFilesCount();

      rowsAdded += manifestFile.addedRowsCount();
      rowsDeleted += manifestFile.deletedRowsCount();
      rowsExisting += manifestFile.existingRowsCount();

      ManifestFile.PartitionFieldSummary partitionFieldSummary = manifestFile.partitions().get(0);
      int lb = Conversions.fromByteBuffer(Types.IntegerType.get(), partitionFieldSummary.lowerBound());
      int ub = Conversions.fromByteBuffer(Types.IntegerType.get(), partitionFieldSummary.upperBound());
      System.out.println(String.format("%s, add: %d, del: %d, exist: %d, [%d, %d]", manifestFile.path(), addedFiles, deletedFiles, existingFiles, lb, ub));
    }

    int fileCount = numExistingFiles + numFilesAdded - numFilesDeleted;
    long rowCount = rowsAdded + rowsExisting - rowsDeleted;
    System.out.println(String.format("NF=%d, TT=%d, Add=%d, Del=%d, Exist=%d, Total=%d, AR=%d, DR=%d, ER=%d, TR=%d",
        TestTables.NUM_FILES_READ.get(), stopwatch.elapsed(TimeUnit.MICROSECONDS),
        numFilesAdded, numFilesDeleted, numExistingFiles, fileCount,
        rowsAdded, rowsDeleted, rowsExisting, rowCount));
  }

  private void runQueries(TestTables.TestTable table, int currentDate) {
    List<Expression> expressions = new ArrayList<>();
    Expression dateEq = Expressions.equal("date", 178);
    Expression storeIdEq = Expressions.equal("storeId", 1);
    //expressions.add(dateEq);
    //expressions.add(storeIdEq);
    expressions.add(Expressions.and(dateEq, storeIdEq));

    // bring all manifest files into page cache
    //table.newScan().filter(storeIdEq).planFiles();

    for(Expression expression : expressions) {
      TestTables.resetCounters();
      Stopwatch stopwatch = Stopwatch.createStarted();
      TableScan tableScan = table.newScan().filter(expression);

      CloseableIterable<FileScanTask> filesToScan = tableScan.planFiles();

      /*
      for (FileScanTask file: filesToScan) {
        System.out.println(file.file().recordCount());
      }*/

      int numFilesScan = Iterables.size(filesToScan);
      long timeTaken = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      float mbsRead = (float) ((TestTables.NUM_BYTES_READ.get() * 1.0) / (1024 * 1024.0));
      System.out.println(String.format("%d, %d, %d, %f, %s, %d", currentDate, timeTaken, TestTables.NUM_FILES_READ.get(), mbsRead, expression, numFilesScan));
      ManifestPartitionPruning mpp = new ManifestPartitionPruning(table, expression);
      mpp.estimate2();
    }
  }

  private List<DataFile> getDateFiles(PartitionSpec partitionSpec, int dateCol, int numStores, int numFiles) {
    List<DataFile> files = new ArrayList<>(numFiles);

    for(int i = 0; i < numFiles; i++) {
      int storeId = i / numStores;
      PartitionData partition = new PartitionData(partitionSpec.partitionType());
      partition.set(0, dateCol);
      partition.set(1, storeId);
        if(storeId == 1 && dateCol == 178) {
          numOfFilesExpected++;
        }

      DataFile file = DataFiles.builder(partitionSpec)
          .withPath("/path/to/file-" + dateCol + "-" + storeId + "-" + i + ".parquet")
          .withFileSizeInBytes(256_000_000L)
          .withPartition(partition)
          .withMetrics(columnMetrics)
          .build();

      files.add(file);
    }

    return files;
  }

  static int deletedStoreId = 0;
  private List<DataFile> getFilesFromAllDates(PartitionSpec partitionSpec, int startDate, int endDate) {
    List<DataFile> files = new ArrayList<>();
    for(int i = startDate; i < endDate; i++) {
      PartitionData partition = new PartitionData(partitionSpec.partitionType());
      partition.set(0, i);
      partition.set(1, deletedStoreId);

      DataFile file = DataFiles.builder(partitionSpec)
          .withPath("/path/to/file-" + i + "-" + deletedStoreId + "-" + i + ".parquet")
          .withFileSizeInBytes(256_000_000L)
          .withPartition(partition)
          .withMetrics(columnMetrics)
          .build();

      files.add(file);
    }

    deletedStoreId++;
    return files;
  }

  private void addAndDeleteFilesForDate(Table table, int dateCol, int numStores, int numFiles, int numDaysToRetain) {
    Transaction transaction = table.newTransaction();
    if (deleteDataFiles) {
      DeleteFiles deleteFiles = transaction.newDelete();
      List<DataFile> filesToDelete = getDateFiles(table.spec(), dateCol - numDaysToRetain, numStores, numFiles);
      //List<DataFile> filesToDelete = getFilesFromAllDates(table.spec(), dateCol - numDaysToRetain, dateCol);
      filesToDelete.forEach(deleteFiles::deleteFile);
      deleteFiles.commit();
      transaction.commitTransaction();
      table.refresh();
    }

    AppendFiles appendFiles = transaction.newAppend();
    List<DataFile> filesToAppend = getDateFiles(table.spec(), dateCol, numStores, numFiles);
    filesToAppend.forEach(appendFiles::appendFile);
    appendFiles.commit();

    transaction.commitTransaction();
    table.refresh();
  }

  private void createFilesForDate(Table table, int numDays, int numStores, int numFiles) {
    AppendFiles appendFiles = table.newAppend();
    for(int i = 1; i <= numDays; i++) {
      List<DataFile> files = getDateFiles(table.spec(), i, numStores, numFiles);
      files.forEach(appendFiles::appendFile);
      if ((i % 30) == 0) {
        appendFiles.commit();
        table.refresh();
        appendFiles = table.newAppend();
      }
    }
    appendFiles.commit();

    table.refresh();
  }

  public static class ManifestPartitionPruning {
    private final TestTables.TestTable testTable;
    private final Expression expression;
    private final long timeLimit = 10;
    long finalFilesCount = 0;
    long finalRecordCount = 0;
    public ManifestPartitionPruning(TestTables.TestTable table, Expression expression) {
      testTable = table;
      this.expression = expression;
    }

    public void estimate() {
      ManifestsTable manifestsTable = new ManifestsTable(testTable.ops(), testTable);

      TableScan scan = manifestsTable.newScan().filter(expression);

      CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
      for (FileScanTask file: fileScanTasks ) {
        System.out.println(file.file());
      }

      System.out.println(Iterables.size(fileScanTasks));
    }

    public void estimate2() {
      Stopwatch stopwatch = Stopwatch.createStarted();
      Map<Integer, PartitionSpec> specsById = testTable.specs();
      LoadingCache<Integer, ManifestEvaluator> evalCache = specsById == null ?
          null : Caffeine.newBuilder().build(specId -> {
        PartitionSpec spec = specsById.get(specId);
        return ManifestEvaluator.forPartitionFilter(
            Projections.inclusive(spec, true).project(expression),
            spec, true);
      });

      List<ManifestFile> manifestFiles = ManifestLists.read(testTable.ops().io().newInputFile(testTable.currentSnapshot().manifestListLocation()));
      Iterable<ManifestFile> matchingManifests = evalCache == null ? manifestFiles : Iterables.filter(manifestFiles, manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
      long timeTaken = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      long timeLeft = timeLimit - timeTaken;
      if(timeLeft <= 2) {

        for(ManifestFile manifestFile : manifestFiles) {
          finalFilesCount = manifestFile.addedFilesCount() + manifestFile.existingFilesCount();
          finalRecordCount = manifestFile.addedRowsCount() + manifestFile.existingRowsCount();
        }
        outputfileEstimate(stopwatch.elapsed(TimeUnit.MILLISECONDS), finalFilesCount, finalRecordCount);
        return;
      }

      Boolean timeOut = false;

      double maxFileRatio = 0;
      double maxRecordRatio = 0;
      int totalRemainManifest = 0;
      for (ManifestFile mf: matchingManifests) {
        if(timeOut == false) {
          Pair pair = getEstimateByManifestFile(mf, stopwatch);
          if(pair == null) {
            timeOut = true;
          }
          if(maxFileRatio > (double)pair.first()) {
            maxFileRatio = Math.max(maxFileRatio, (double)pair.first());
            maxRecordRatio = Math.max(maxRecordRatio, (double) pair.second());
          }
        } else {
          long estimatedFileCount = (long)(maxFileRatio * (mf.addedFilesCount() + mf.existingFilesCount()));
          long estimatedRecordCount = (long)(maxRecordRatio * (mf.addedRowsCount() + mf.existingRowsCount()));
          finalRecordCount += estimatedFileCount;
          finalFilesCount += estimatedRecordCount;
        }
      }
      outputfileEstimate(stopwatch.elapsed(TimeUnit.MILLISECONDS), finalFilesCount, finalRecordCount);
      System.out.println("EndTime" + timeTaken);
      System.out.println("ManifestFiles" + Iterables.size(manifestFiles) + " MatchingManifestFiles "  + Iterables.size(matchingManifests));
    }

    public Pair<Double, Double> getEstimateByManifestFile(ManifestFile manifestFile, Stopwatch stopwatch) {
      InputFile file = testTable.ops().io().newInputFile(manifestFile.path());
      ManifestReader.FileType content = ManifestReader.FileType.DATA_FILES;

      int specId = manifestFile.partitionSpecId();
      Map<Integer, PartitionSpec> specsById = testTable.specs();
      PartitionSpec spec = specsById.get(specId);
      AvroIterable<ManifestEntry<DataFile>> reader = Avro.read(file)
          .project(ManifestEntry.getSchema(spec.partitionType()))
          .rename("manifest_entry", GenericManifestEntry.class.getName())
          .rename("partition", PartitionData.class.getName())
          .rename("r102", PartitionData.class.getName())
          .rename("data_file", GenericDataFile.class.getName())
          .rename("r2", GenericDataFile.class.getName())
          .classLoader(GenericManifestEntry.class.getClassLoader())
          .reuseContainers()
          .build();

      Evaluator evaluator = new Evaluator(spec.partitionType(), Projections.inclusive(spec, true).project(expression), true);
      long totalFilesMatched = 0;
      long totalRecordsMatch = 0;
      for (ManifestEntry<DataFile> entry: reader) {
        if(stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeLimit) {
          return null;
        }
        DataFile df = entry.file();
        Boolean matching =  df != null && evaluator.eval(df.partition());
        if(matching == true) {
          totalFilesMatched++;
          totalRecordsMatch += df.recordCount();
        }
      }

      long totalDataFiles = manifestFile.addedFilesCount() + manifestFile.existingFilesCount();
      long totalRecordCount = manifestFile.addedRowsCount() + manifestFile.existingRowsCount();
      double fileRatio = totalFilesMatched/ totalDataFiles;
      double recordRatio = totalRecordsMatch/totalRecordCount;
      finalFilesCount += totalFilesMatched;
      finalRecordCount += totalRecordsMatch;
      return Pair.of(fileRatio, recordRatio);
      //System.out.println("For ManifestFIle : " + manifestFile.path());
      //System.out.println("Total Files matched: " + totalFilesMatched + " out of " + totalDataFiles + " total files");
      //System.out.println("Total Records matched: " + totalRecordsMatch + " out of " + totalRecordCount + " total files");

    }

    public void outputfileEstimate(long timeTaken, long files, long records) {
      System.out.println("**** YOU MAY READ " + files + " files and " + records + " records. Total Time taken to estimate this: " + timeTaken);
    }

    public void getEstimatePartitionStatsFile() {

    }

    protected OutputFile newPartitionStatsFile(int specId) {
      return testTable.ops().io().newOutputFile(testTable.ops().metadataFileLocation(FileFormat.AVRO.addExtension(
          String.format("partitionStats-%d", specId))));
    }
  }

}
