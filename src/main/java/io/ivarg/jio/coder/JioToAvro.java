package io.ivarg.jio.coder;

import static java.util.stream.Collectors.toMap;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class JioToAvro {

  public static <T> GenericRecord convert(T value) {
    Schema schema = generateSchema(value.getClass());
    var record = new GenericRecordBuilder(schema);

    var fieldMap = getFieldMap(value);
    fieldMap.values().stream()
        .forEach(
            fld -> {
              try {
                var fieldSchema = schema.getField(fld.getName()).schema();
                if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
                  var nestedValue = fld.get(value);
                  var nestedRecord = convert(nestedValue);
                  record.set(fld.getName(), nestedRecord);
                } else if (fld.getType().equals(Optional.class)) {
                  var opt = (Optional) fld.get(value);
                  if (opt.isPresent()) {
                    var realValue = opt.get();
                    if (fieldSchema.getTypes().get(1).getType().equals(Schema.Type.RECORD)) {
                      var nestedRecord = convert(realValue);
                      record.set(fld.getName(), nestedRecord);
                    } else {
                      record.set(fld.getName(), realValue);
                    }
                  }
                } else {
                  record.set(fld.getName(), fld.get(value));
                }
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
    return record.build();
  }

  private static Map<String, Field> getFieldMap(Object obj) {
    Arrays.stream(obj.getClass().getDeclaredFields()).forEach(f -> f.setAccessible(true));
    var allFields = obj.getClass().getDeclaredFields();
    Arrays.stream(allFields).forEach(f -> f.setAccessible(true));
    return Arrays.stream(allFields)
        .map(f -> Map.entry(f.getName(), f))
        .collect(toMap(Entry::getKey, Entry::getValue));
  }

  public static <T> Schema generateSchema(Class<T> type) {
    var schema = SchemaBuilder.record(type.getSimpleName()).fields();

    var declaredFields = List.of(type.getDeclaredFields());
    for (Field field : declaredFields) {
      var newSchema = addPrimitiveField(schema, field.getType(), field.getName(), true);
      if (newSchema != null) {
        schema = newSchema;
        continue;
      }

      var fieldType = field.getType();
      if (fieldType.getTypeName().equals("java.util.Optional")) {
        var paramType = (ParameterizedType) field.getGenericType();
        var optionalType = (Class<?>) paramType.getActualTypeArguments()[0];

        newSchema = addPrimitiveField(schema, optionalType, field.getName(), false);
        if (newSchema != null) {
          schema = newSchema;
          continue;
        }

        // The field is a optional record
        var recordSchema = generateSchema(optionalType);
        schema = schema.name(field.getName()).type().optional().type(recordSchema);
        continue;
      }

      // The field is a record
      var recordSchema = generateSchema(fieldType);
      schema = schema.name(field.getName()).type(recordSchema).noDefault();
    }

    return schema.endRecord();
  }

  private static FieldAssembler<Schema> addPrimitiveField(
      FieldAssembler<Schema> schema, Class<?> fieldType, String fieldName, boolean required) {
    switch (fieldType.getTypeName()) {
      case "java.lang.String":
        return required ? schema.requiredString(fieldName) : schema.optionalString(fieldName);
      case "java.lang.Boolean":
      case "boolean":
        return required ? schema.requiredBoolean(fieldName) : schema.optionalBoolean(fieldName);
      case "int":
      case "java.lang.Integer":
        return required ? schema.requiredInt(fieldName) : schema.optionalInt(fieldName);
      case "long":
      case "java.lang.Long":
        return required ? schema.requiredLong(fieldName) : schema.optionalLong(fieldName);
      case "float":
      case "java.lang.Float":
        return required ? schema.requiredFloat(fieldName) : schema.optionalFloat(fieldName);
      case "double":
      case "java.lang.Double":
        return required ? schema.requiredDouble(fieldName) : schema.optionalDouble(fieldName);
      case "byte[]":
        return required ? schema.requiredBytes(fieldName) : schema.optionalBytes(fieldName);
      default:
        return null;
    }
  }
}
