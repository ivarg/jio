package io.ivarg.jio.coder;

import static java.util.stream.Collectors.toMap;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A custom implementation of GenericRecord to POJO conversion.
 *
 * <p>The POJO can represent only parts of the GenericRecord, but must otherwise match the recurd's
 * structure, field names, and types.
 *
 * <p>While I figure there must be some library for this out there, I haven't found anything
 * appropriate. Closest I've seen is using the Spring framework, as suggested here:
 * https://karengryg.io/2018/08/25/avro-and-pojo-conversionstips-for-kafka-devs/ However, it doesn't
 * match 100% what I want to do here, and I also don't want to pull in Spring only for this.
 */
public class AvroToJio {

  /**
   * Convert the relevant parts of a GenericRecord to an instance of a given class.
   *
   * <p>This is done by matching each field in the class to its corresponding value in the record.
   * All required values in the GenericRecord does not have to be represented in the class. Union
   * types are accepted only as single, nullable types. I.e. other union variants found in the
   * record will generate an exception.
   */
  public static <RType> RType convert(GenericRecord record, Class<RType> tClass) throws Exception {
    // for each field in tClass, find the corresponding value in record.
    // Use values to create an instance of tClass
    // Do this recursively for each record type field
    Schema schema = record.getSchema();
    return convertRecord(record, schema, tClass);
  }

  private static <RType> RType convertRecord(
      GenericRecord record, Schema schema, Class<RType> aClass) {

    var allFields = aClass.getDeclaredFields();
    var fieldMap =
        Arrays.stream(allFields)
            .map(f -> Map.entry(f.getName(), f))
            .collect(toMap(Entry::getKey, Entry::getValue));
    Arrays.stream(allFields).forEach(f -> f.setAccessible(true));

    try {
      var ctor = aClass.getDeclaredConstructor();
      ctor.setAccessible(true);
      RType instance = aClass.getDeclaredConstructor().newInstance();

      var schemaFields =
          schema.getFields().stream()
              .map(f -> Map.entry(f.name(), f.schema()))
              .collect(toMap(Entry::getKey, Entry::getValue));
      fieldMap.values().stream()
          .forEach(
              fld -> {
                if (!schemaFields.containsKey(fld.getName())) {
                  throw new RuntimeException(
                      String.format("class field '%s' not present in schema", fld.getName()));
                }
                Object val = convertFieldValue(record, schemaFields.get(fld.getName()), fld);
                try {
                  fld.set(instance, val);
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(e);
                }
              });

      return instance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Optional<Object> convertUnion(
      GenericRecord record, Schema fieldSchema, Field field) {
    var typeList = fieldSchema.getTypes();

    if (!typeList.get(0).getType().equals(Schema.Type.NULL)) {
      throw new RuntimeException("Cannot handle record unions where first type is not null");
    }
    if (typeList.size() > 2) {
      throw new RuntimeException(
          "Cannot handle record unions with more than 2 types (null, <type>)");
    }
    if (!field.getType().equals(Optional.class)) {
      throw new RuntimeException("Converted field must be of type Optional");
    }

    if (record.get(field.getName()) == null) {
      return Optional.empty();
    }

    return Optional.of(convertFieldValue(record, typeList.get(1), field));
  }

  // Given an avro record, its schema, and a field of a POJO,
  // return the corresponding field value of the record
  private static Object convertFieldValue(GenericRecord record, Schema fieldSchema, Field field) {
    // Check field type, start with complex types
    switch (fieldSchema.getType()) {
        // We only deal with unions that encode a single, nullable type
      case UNION:
        return convertUnion(record, fieldSchema, field);

        // it's not union, check if it's a record
      case RECORD:
        Class<?> nestedClass;
        if (field.getType().equals(Optional.class)) {
          var paramType = (ParameterizedType) field.getGenericType();
          nestedClass = (Class<?>) paramType.getActualTypeArguments()[0];
        } else {
          nestedClass = field.getType();
        }
        GenericRecord newRecord = (GenericRecord) record.get(field.getName());

        return convertRecord(newRecord, fieldSchema, nestedClass);

        // We have checked all complex field type (union, record) -> it must be a primitive
        // non-nullable type,
      default:
        return extractPrimitiveValue(record, fieldSchema, field);
    }
  }

  private static Object extractPrimitiveValue(
      GenericRecord record, Schema fieldSchema, Field field) {
    Object rawVal = record.get(field.getName());

    Class<?> targetCls;
    switch (fieldSchema.getType()) {
      case STRING:
        targetCls = String.class;
        rawVal = String.valueOf(rawVal);
        break;
      case BOOLEAN:
        targetCls = Boolean.class;
        break;
      case LONG:
        targetCls = Long.class;
        break;
      case INT:
        targetCls = Integer.class;
        break;
      case FLOAT:
        targetCls = Float.class;
        break;
      case DOUBLE:
        targetCls = Double.class;
        break;
      case BYTES:
        targetCls = byte[].class;
        break;
      default:
        throw new RuntimeException(fieldSchema.getType() + " is not a primitive type");
    }
    if (!targetCls.isAssignableFrom(rawVal.getClass())) {
      throw new RuntimeException(
          "type error: expected " + targetCls + " but got " + rawVal.getClass());
    }
    return rawVal;
  }
}
