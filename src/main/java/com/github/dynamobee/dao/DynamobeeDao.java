package com.github.dynamobee.dao;

import com.github.dynamobee.changeset.ChangeEntry;
import com.github.dynamobee.exception.DynamobeeLockException;
import com.github.dynamobee.utils.DynamoDbEnhancedTableSchemaUtils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;


public class DynamobeeDao {
  private static final Logger logger = LoggerFactory.getLogger("Dynamobee dao");

  static final TableSchema<ChangeEntry> CHANGE_ENTRY_TABLE_SCHEMA = TableSchema.fromImmutableClass(ChangeEntry.class);

  private static final String VALUE_LOCK = "LOCK";
  private static final ChangeEntry LOCK_ITEM = ChangeEntry.builder()
      .setChangeId(VALUE_LOCK)
      .build();

  private DynamoDbClient dynamoDbClient;
  private DynamoDbEnhancedClient dynamoDbEnhancedClient;
  private String dynamobeeTableName;
  private DynamoDbTable<ChangeEntry> dynamobeeTable;
  private boolean waitForLock;
  private long changeLogLockWaitTime;
  private long changeLogLockPollRate;
  private boolean throwExceptionIfCannotObtainLock;

  public DynamobeeDao(String dynamobeeTableName, boolean waitForLock, long changeLogLockWaitTime,
                      long changeLogLockPollRate, boolean throwExceptionIfCannotObtainLock) {
    this.dynamobeeTableName = dynamobeeTableName;
    this.waitForLock = waitForLock;
    this.changeLogLockWaitTime = changeLogLockWaitTime;
    this.changeLogLockPollRate = changeLogLockPollRate;
    this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
  }

  public void connectDynamoDB(DynamoDbClient dynamoDbClient) {
    this.dynamoDbClient = dynamoDbClient;
    this.dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder()
        .dynamoDbClient(dynamoDbClient)
        .build();

    dynamobeeTable = findOrCreateDynamoBeeTable();
  }

  private DynamoDbTable<ChangeEntry> findOrCreateDynamoBeeTable() {
    logger.info("Searching for an existing DynamoBee table; please wait...");

    try {
      dynamoDbClient.describeTable(describeTableRequest -> describeTableRequest
              .tableName(dynamobeeTableName))
          .table();
      logger.info("DynamoBee table found");
      return dynamoDbEnhancedClient.table(dynamobeeTableName, CHANGE_ENTRY_TABLE_SCHEMA);
    } catch (ResourceNotFoundException rnfe) {
      logger.info("Attempting to create DynamoBee table; please wait...");
      var table = dynamoDbEnhancedClient.table(dynamobeeTableName, CHANGE_ENTRY_TABLE_SCHEMA);
      table.createTable();
      var waiterResult = dynamoDbClient.waiter()
          .waitUntilTableExists(builder -> builder.tableName(dynamobeeTableName))
          .matched();

      waiterResult.exception().ifPresent(throwable ->
          logger.error("Failure. DynamoBee Table status: {}", TableStatus.CREATING, throwable));

      waiterResult.response().ifPresent(response ->
          logger.info("Success. DynamoBee Table status: {}", response.table().tableStatus()));
      return table;
    }
  }

  /**
   * Try to acquire process lock
   *
   * @return true if successfully acquired, false otherwise
   * @throws DynamobeeLockException exception
   */
  public boolean acquireProcessLock() throws DynamobeeLockException {
    boolean acquired = this.acquireLock();

    if (!acquired && waitForLock) {
      long timeToGiveUp = new Date().getTime() + (changeLogLockWaitTime * 1000 * 60);
      while (!acquired && new Date().getTime() < timeToGiveUp) {
        acquired = this.acquireLock();
        if (!acquired) {
          logger.info("Waiting for changelog lock....");
          try {
            Thread.sleep(changeLogLockPollRate * 1000);
          } catch (InterruptedException e) {
            // nothing
          }
        }
      }
    }

    if (!acquired && throwExceptionIfCannotObtainLock) {
      logger.info("Dynamobee did not acquire process lock. Throwing exception.");
      throw new DynamobeeLockException("Could not acquire process lock");
    }

    return acquired;
  }

  public boolean acquireLock() {

    // acquire lock by attempting to insert the same value in the collection - if it already exists (i.e. lock held)
    // there will be an exception
    try {
      this.dynamobeeTable.putItem(requestConsumer -> requestConsumer
          .item(ChangeEntry
              .builder()
              .setChangeId(VALUE_LOCK)
              .setTimestamp(new Date())
              .setAuthor(getHostName())
              .build())
          .conditionExpression(DynamoDbEnhancedTableSchemaUtils.notExists(CHANGE_ENTRY_TABLE_SCHEMA)));
    } catch (ConditionalCheckFailedException ex) {
      logger.warn("The lock has been already acquired.");
      return false;
    }
    return true;
  }

  private String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "UnknownHost";
    }
  }

  public void releaseProcessLock() {
    this.dynamobeeTable.deleteItem(LOCK_ITEM);
  }

  public boolean isProccessLockHeld() {
    return this.dynamobeeTable.getItem(LOCK_ITEM) != null;
  }

  public boolean isNewChange(ChangeEntry changeEntry) {
    return this.dynamobeeTable.getItem(changeEntry) == null;
  }

  public void save(ChangeEntry changeEntry) {
    this.dynamobeeTable.putItem(changeEntry);
  }

  public void setChangelogTableName(String changelogCollectionName) {
    this.dynamobeeTableName = changelogCollectionName;
  }

  public boolean isWaitForLock() {
    return waitForLock;
  }

  public void setWaitForLock(boolean waitForLock) {
    this.waitForLock = waitForLock;
  }

  public long getChangeLogLockWaitTime() {
    return changeLogLockWaitTime;
  }

  public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
    this.changeLogLockWaitTime = changeLogLockWaitTime;
  }

  public long getChangeLogLockPollRate() {
    return changeLogLockPollRate;
  }

  public void setChangeLogLockPollRate(long changeLogLockPollRate) {
    this.changeLogLockPollRate = changeLogLockPollRate;
  }

  public boolean isThrowExceptionIfCannotObtainLock() {
    return throwExceptionIfCannotObtainLock;
  }

  public void setThrowExceptionIfCannotObtainLock(boolean throwExceptionIfCannotObtainLock) {
    this.throwExceptionIfCannotObtainLock = throwExceptionIfCannotObtainLock;
  }

}
