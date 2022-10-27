package com.xm4399.flink.datagen;

import com.xm4399.flink.constant.Types;
import com.xm4399.flink.utils.EncodingUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.factories.DataGenOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class DataGenerator<T> {
    public static final String IDENTIFIER = "datagen";
    private final StreamTableEnvironment tableEnv;
    private final CatalogManager catalogManager;

    public DataGenerator(StreamExecutionEnvironment env) {
        this.tableEnv = StreamTableEnvironment.create(env);
        TableEnvironmentImpl tableEnvImpl = (TableEnvironmentImpl) tableEnv;
        this.catalogManager = tableEnvImpl.getCatalogManager();
    }

    public DataStream<T> toDataStream(DataGenDescriptor descriptor){
        initializeDataGenTable(descriptor);
        Table table = queryDataGenTable(descriptor.getPojoClass().getSimpleName());
        return (DataStream<T>) tableEnv.toDataStream(table, descriptor.getPojoClass());
    }

    private void initializeDataGenTable(DataGenDescriptor descriptor){
        Schema schema = getSchema(descriptor);
        CatalogTable catalogTable = CatalogTable.of(
                schema,
                null,
                Collections.emptyList(),
                getConnectorOptions(descriptor)
        );
        String tableOptions = catalogTable.getOptions().entrySet().stream()
                .map(e -> EncodingUtils.stringifyOption(e.getKey(), e.getValue()))
                .collect(Collectors.joining("\n"));

        String tableName = descriptor.getPojoClass().getSimpleName();
        System.out.printf("Table `%s`\n  %s \n %s%n%n", tableName, schema, tableOptions);

        catalogManager.createTemporaryTable(
                catalogTable,
                ObjectIdentifier.of(
                        catalogManager.getCurrentCatalog(),
                        catalogManager.getCurrentDatabase(),
                        tableName
                ),
                false
        );
    }

    private Table queryDataGenTable(String tableName){
        return tableEnv.sqlQuery("select * from " + tableName);
    }

    private Schema getSchema(DataGenDescriptor descriptor){
        final List<Column> columns= new ArrayList<>();
        Class<?> pojoClass = descriptor.getPojoClass();
        for (Field field : pojoClass.getDeclaredFields()) {
            String name = field.getName();
            String type = field.getType().getSimpleName();
            DataType dataType;
            switch (type.toUpperCase()){
                case Types.STRING:
                    dataType = DataTypes.STRING();
                    break;
                case Types.INTEGER:
                    dataType = DataTypes.INT();
                    break;
                case Types.LONG:
                    dataType = DataTypes.BIGINT();
                    break;
                case Types.BOOLEAN:
                    dataType = DataTypes.BOOLEAN();
                    break;
                case Types.BIGDECIMAL:
                    dataType = DataTypes.DECIMAL(15,4);
                    break;
                case Types.FLOAT:
                    dataType = DataTypes.FLOAT();
                    break;
                case Types.DOUBLE:
                    dataType = DataTypes.DOUBLE();
                    break;
                case Types.DATE:
                    dataType = DataTypes.DATE();
                    break;
                case Types.DATETIME:
                case Types.TIMESTAMP:
                    dataType = DataTypes.TIMESTAMP(3);
                    break;
                default:
                    throw new RuntimeException(String.format("Data type %s is not supported yet",type));
            }
            Column.PhysicalColumn physicalColumn = Column.physical(name, dataType);
            columns.add(physicalColumn);
        }
        return Schema.newBuilder().fromResolvedSchema(ResolvedSchema.of(columns)).build();
    }

    private Map<String,String> getConnectorOptions(DataGenDescriptor descriptor){
        Map<String, String> options = new LinkedHashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        if (descriptor.getRowsPerSecond()!=null){
            options.put(DataGenOptions.ROWS_PER_SECOND.key(), descriptor.getRowsPerSecond().toString());
        }
        if (descriptor.getNumberOfRows()!=null){
            options.put(DataGenOptions.NUMBER_OF_ROWS.key(), descriptor.getNumberOfRows().toString());
        }
        Class<?> pojoClass = descriptor.getPojoClass();
        for (Field field : pojoClass.getDeclaredFields()) {
            String name = field.getName();
            String type = field.getType().getSimpleName().toUpperCase();
            if (field.isAnnotationPresent(DataGenOption.class)){
                DataGenOption annotation = field.getAnnotation(DataGenOption.class);
                if (DataGenOptions.RANDOM.equals(annotation.kind())){
                    options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.KIND, DataGenOptions.RANDOM);
                    if (Types.STRING.equals(type)){
                        options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.LENGTH, String.valueOf(annotation.length()));
                    }
                    if (Types.isNumericType(type)){
                        options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.MIN,String.valueOf(annotation.min()));
                        options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.MAX,String.valueOf(annotation.max()));
                    }
                }
                if (DataGenOptions.SEQUENCE.equals(annotation.kind())){
                    options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.KIND, DataGenOptions.SEQUENCE);
                    if (Types.isNumericType(type) || Types.STRING.equals(type)) {
                        options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.START,String.valueOf(annotation.start()));
                        options.put(DataGenOptions.FIELDS+"."+name+"."+DataGenOptions.END,String.valueOf(annotation.end()));
                    }
                }
            }
        }
        return options;
    }

    protected StreamTableEnvironment getTableEnv(){
        return tableEnv;
    }



}
