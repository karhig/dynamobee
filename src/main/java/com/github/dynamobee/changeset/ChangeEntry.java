package com.github.dynamobee.changeset;

import com.github.dynamobee.Dynamobee;
import com.github.dynamobee.utils.DynamoDbEnhancedTableSchemaUtils;
import java.util.Date;
import software.amazon.awssdk.enhanced.dynamodb.DefaultAttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbImmutable;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;


/**
 * Entry in the changes collection log {@link Dynamobee#DEFAULT_CHANGELOG_TABLE_NAME}
 * Type: entity class.
 */
@DynamoDbImmutable(
    builder = ChangeEntry.Builder.class,
    converterProviders = {
    DynamoDbEnhancedTableSchemaUtils.DynamoBeeConverterProvider.class,
    DefaultAttributeConverterProvider.class})
public class ChangeEntry {
  private final String changeId;
  private final String author;
  private final Date timestamp;
  private final String changeLogClass;
  private final String changeSetMethodName;

  private ChangeEntry(Builder b) {
    this.changeId = b.changeId;
    this.author = b.author;
    this.timestamp = b.timestamp;
    this.changeLogClass = b.changeLogClass;
    this.changeSetMethodName = b.changeSetMethodName;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "[ChangeSet: id=" + this.changeId +
        ", author=" + this.author +
        ", timestamp=" + this.timestamp +
        ", changeLogClass=" + this.changeLogClass +
        ", changeSetMethod=" + this.changeSetMethodName + "]";
  }

  @DynamoDbPartitionKey
  @DynamoDbAttribute("changeId")
  public String getChangeId() {
    return this.changeId;
  }

  @DynamoDbAttribute("author")
  public String getAuthor() {
    return this.author;
  }

  @DynamoDbAttribute("timestamp")
  public Date getTimestamp() {
    return this.timestamp;
  }

  @DynamoDbAttribute("changeLogClass")
  public String getChangeLogClass() {
    return this.changeLogClass;
  }

  @DynamoDbAttribute("changeSetMethod")
  public String getChangeSetMethodName() {
    return this.changeSetMethodName;
  }

  public static final class Builder {
    private String changeId;
    private String author;
    private Date timestamp;
    private String changeLogClass;
    private String changeSetMethodName;

    private Builder() {
      // Only created via ChangeEntry.builder()
    }

    public Builder setChangeId(String changeId) {
      this.changeId = changeId;
      return this;
    }

    public Builder setAuthor(String author) {
      this.author = author;
      return this;
    }

    public Builder setTimestamp(Date timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder setChangeLogClass(String changeLogClass) {
      this.changeLogClass = changeLogClass;
      return this;
    }

    public Builder setChangeSetMethodName(String changeSetMethodName) {
      this.changeSetMethodName = changeSetMethodName;
      return this;
    }

    public ChangeEntry build() {
      return new ChangeEntry(this);
    }
  }

}
