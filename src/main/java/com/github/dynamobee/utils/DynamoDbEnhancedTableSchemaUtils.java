package com.github.dynamobee.utils;

import java.util.Date;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDbEnhancedTableSchemaUtils {

  private DynamoDbEnhancedTableSchemaUtils() {
  }

  public static Expression notExists(TableSchema<?> schema) {
    var builder = Expression.builder();
    var partitionKeyExpressionName = "#partition_key";
    builder.putExpressionName(partitionKeyExpressionName, schema.tableMetadata().primaryPartitionKey());
    schema.tableMetadata().primarySortKey().ifPresentOrElse(
        sortKey -> {
          var sortKeyExpressionName = "#sort_key";
          builder.putExpressionName(sortKeyExpressionName, sortKey);
          builder.expression("attribute_not_exists(#partition_key) AND attribute_not_exists(#sort_key)");
        },
        () -> builder.expression("attribute_not_exists(#partition_key)"));
    return builder
        .build();
  }

  public static final class DynamoBeeConverterProvider implements AttributeConverterProvider {

    @SuppressWarnings("unchecked")
    @Override
    public <T> AttributeConverter<T> converterFor(
        EnhancedType<T> enhancedType) {
      if (enhancedType.rawClass().isAssignableFrom(Date.class)) {
        return (AttributeConverter<T>) new DateConverter();
      } else {
        return null;
      }
    }

    public static final class DateConverter implements AttributeConverter<Date> {

      @Override
      public AttributeValue transformFrom(Date input) {
        return AttributeValue.builder().n(Long.toString(input.getTime())).build();
      }

      @Override
      public Date transformTo(AttributeValue input) {
        return new Date(Long.parseLong(input.n()));
      }

      @Override
      public EnhancedType<Date> type() {
        return EnhancedType.of(Date.class);
      }

      @Override
      public AttributeValueType attributeValueType() {
        return AttributeValueType.N;
      }
    }
  }

}
