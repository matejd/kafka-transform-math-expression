package com.github.matejd.kafka.connect.transform;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import com.udojava.evalex.Expression;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MathExpression<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Evaluates a math expression";

    private interface ConfigName {
        String EXPRESSION = "expression";
        String FIELD_NAME = "field.name";
        String FIELD_TYPE = "field.type";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            ConfigName.EXPRESSION,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Math expression"
        )
        .define(
            ConfigName.FIELD_NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.MEDIUM,
            "Target field name"
        )
        .define(
            ConfigName.FIELD_TYPE,
            ConfigDef.Type.STRING,
            "string",
            ConfigDef.Importance.MEDIUM,
            "Target field type"
        );

    private static final String PURPOSE = "math expression";

    private String expression;
    private String fieldName;
    private String fieldType;

    private Map<String, Schema> casts;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        expression = config.getString(ConfigName.EXPRESSION);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        fieldType = config.getString(ConfigName.FIELD_TYPE);

        casts = new HashMap<>();
        casts.put("int64", Schema.INT64_SCHEMA);
        casts.put("float64", Schema.FLOAT64_SCHEMA);
        casts.put("string", Schema.STRING_SCHEMA);
        casts.put("int64?", Schema.OPTIONAL_INT64_SCHEMA);
        casts.put("float64?", Schema.OPTIONAL_FLOAT64_SCHEMA);
        casts.put("string?", Schema.OPTIONAL_STRING_SCHEMA);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        updatedValue.put(fieldName, evaluateExpression(record));

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(fieldName, evaluateExpression(record));

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private Object evaluateExpression(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Expression exp = new Expression(expression);
        List<String> variables = exp.getUsedVariables();
        for (String variable : variables) {
            exp.with(variable, value.get(variable).toString());
        }

        BigDecimal result = exp.eval();

        Schema schema = casts.get(fieldType);

        if (schema.equals(Schema.INT64_SCHEMA) || schema.equals(Schema.OPTIONAL_INT64_SCHEMA)) {
            return (long) Double.parseDouble(result.toPlainString());
        } else if (schema.equals(Schema.FLOAT64_SCHEMA) || schema.equals(Schema.OPTIONAL_FLOAT64_SCHEMA)) {
            return Double.parseDouble(result.toPlainString());
        } else {
            return result.toPlainString();
        }
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (field.name() == fieldName) {
                continue;
            }
            builder.field(field.name(), field.schema());
        }

        builder.field(fieldName, casts.get(fieldType));

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends MathExpression<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends MathExpression<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp());
        }

    }
}
