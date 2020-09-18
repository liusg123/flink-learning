package com.zhisheng.connectors;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.zhisheng.connectors.pojo.DevDetailPojo;
import com.zhisheng.connectors.pojo.DevValidPojo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.mapred.JobConf;
import org.bson.BSONObject;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Slf4j
public class MainApplication{

    public static void main(String[] args) throws Exception {
        run(args);
    }

    private static void run(String... args) throws Exception {
        String unitId = "02";
        String statusDate = "2020-06-20";
        String startDate = statusDate + " 00:00:00";
        String endDate = statusDate + " 23:59:59";
        String mongoUrl = "mongodb://zbjs_readwrite:zbjs_readwrite@192.168.2.65:27017/zbjsdb.%s";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        env.setParallelism(2);

        DataSource<Row> devValidDs = createInputDataSource(env,unitId);

        MapOperator<Row, DevValidPojo> devMap = devValidDs.map(devStr -> {
            return new DevValidPojo((String) devStr.getField(0), (String) devStr.getField(1), (String) devStr.getField(2), (String) devStr.getField(3),
                    (Date) devStr.getField(4), (String) devStr.getField(5),
                    (String) devStr.getField(6), (String) devStr.getField(7));
        });

        //long devCount = devMap.count();
        // create a MongodbInputFormat, using a Hadoop input format wrapper
        HadoopInputFormat<BSONWritable, BSONWritable> hdIf = new HadoopInputFormat<BSONWritable, BSONWritable>(new MongoInputFormat(), BSONWritable.class,
                BSONWritable.class, new JobConf());

        // specify connection parameters
        hdIf.getJobConf().set("mongo.input.uri", String.format(mongoUrl, "T_IC_POWERCUT_HIS"));
        hdIf.getJobConf().set("mongo.input.split.create_input_splits", "false");
        //查询时因测试数据不好找停电的就只能找了些不停电的
        hdIf.getJobConf().set("mongo.input.query", String.format("{'UNIT_ID':'%s','TERMINAL_STATE':%s}", unitId, 0));

        DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);
        // a little example how to use the data in a mapper.
        DataSet<Tuple2<String, BSONWritable>> fin = input.map(
                new MapFunction<Tuple2<BSONWritable, BSONWritable>,
                        Tuple2<String, BSONWritable>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, BSONWritable> map(
                            Tuple2<BSONWritable, BSONWritable> record) throws Exception {
                        BSONWritable value = record.getField(1);
                        BSONObject doc = value.getDoc();
                       /* BasicDBObject jsonld = (BasicDBObject) doc.get("jsonld");
                        String id = jsonld.getString("@id");
                        DBObject builder = BasicDBObjectBuilder.start().add("id", id).add("type", jsonld.getString("@type")).get();*/

                        String terminalId = (String) doc.get("TERMINAL_ID");

                        DBObject builder = BasicDBObjectBuilder.start()
                                .add("TERMINAL_ID", terminalId)
                                .add("TRANSFORMER_ID", doc.get("TRANSFORMER_ID"))
                                .add("TERMINAL_STATE", doc.get("TERMINAL_STATE"))   //
                                .add("ACTION_DATE", doc.get("ACTION_DATE"))
                                .add("UNIT_ID", doc.get("UNIT_ID"))
                                .add("DATA_TYPE", doc.get("DATA_TYPE"))
                                .get();

                        BSONWritable w = new BSONWritable(builder);
                        return new Tuple2<String, BSONWritable>(terminalId, w);
                    }
                });
        JoinOperator<DevValidPojo, Tuple2<String, BSONWritable>, DevDetailPojo> joinOper =
                devMap.join(fin).where("devId").equalTo(ks -> {
                    return ks.f0;
                }).with((a, b) -> {
                    BSONObject bw = b.f1.getDoc();
                    return new DevDetailPojo(a.getId(), a.getDevId(), (String) bw.get("TRANSFORMER_ID"), a.getDevType(), a.getDevValid(), a.getValidDate(),
                            (Date) bw.get("ACTION_DATE"),
                            a.getUnitId(), (long) bw.get("DATA_TYPE"), a.getOrgId(), a.getOrgName(), 1);
                }).setParallelism(4);
        ReduceOperator<DevDetailPojo> devGroup = joinOper.groupBy(DevDetailPojo::getTerminalId).reduce((r, d) -> {
            long times = r.getTimes() + d.getTimes();
            r.setTimes(times);
            return r;
        });
        //devGroup.writeAsText("D:\\abc.txt");

        //devGroup.print();
        //fin.print();
        DataSink<DevDetailPojo> ds = createOutput(devGroup);
        env.execute("zbjs unit test");
    }
    private static DataSource<Row> createInputDataSource(ExecutionEnvironment env,String unitId){
        //定义设备类型
        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DATE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
        };

        RowTypeInfo devTypeInfo = new RowTypeInfo(fieldTypes);
        JDBCInputFormat.JDBCInputFormatBuilder mgds = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.postgresql.Driver").setDBUrl("jdbc:postgresql://192.168.2.71:54321/zbjsdb")
                .setUsername("zbjs_readwrite").setPassword("zbjs_readwrite");

        //获取TTU信息
        JDBCInputFormat pgDevvalidFormat = mgds.setQuery(String.format("SELECT ID,DEV_ID,DEV_TYPE,DEV_VALID,VALID_DATE,ORG_ID,ORG_NAME,UNIT_ID FROM T_IC_DEV_VALID " +
                "WHERE UNIT_ID = '%s' AND DEV_TYPE = 'TTU'", unitId)).setRowTypeInfo(devTypeInfo).finish();
        DataSource<Row> devValidDs = env.createInput(pgDevvalidFormat);
        return devValidDs;
    }
    private static DataSink<DevDetailPojo> createOutput(ReduceOperator<DevDetailPojo> devGroup) {
        return devGroup.output(new RichOutputFormat<DevDetailPojo>(){
            private MongoClient mongoClient;

            @Override
            public void configure(Configuration parameters) {

            }
            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                log.error("打开连接====================="+taskNumber+">>>>>>>>"+numTasks);
                ServerAddress serverAddress = new ServerAddress("192.168.2.65", 27017);
                List<MongoCredential> credential = new ArrayList<>();
                //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
                MongoCredential mongoCredential1 = MongoCredential.createCredential("zbjs_readwrite", "zbjsdb", "zbjs_readwrite".toCharArray());
                credential.add(mongoCredential1);
                //通过连接认证获取MongoDB连接
                this.mongoClient = new MongoClient(serverAddress, credential);
            }
            @Override
            public void writeRecord(DevDetailPojo record) throws IOException {
                MongoCollection<Document> col = mongoClient.getDatabase("zbjsdb").getCollection("FLINK_TEST_DATA");
                Document document = new Document();
                document.append("TERMINAL_ID", record.getTerminalId())
                        .append("TRANSFORMER_ID", record.getTransformerId())
                        .append("DEV_TYPE", record.getDevType())   //
                        .append("DEV_VALID", record.getDevValid())
                        .append("ACTION_DATE", record.getActionDate())
                        .append("DATA_TYPE", record.getDataType())
                        .append("ORG_ID", record.getOrgId())
                        .append("ORG_NAME", record.getOrgName())
                        .append("UNIT_ID", record.getUnitId())
                        .append("VALID_DATE", record.getValidDate())
                        .append("TIMES", record.getTimes());
                col.insertOne(document);
            }

            @Override
            public void close() throws IOException {
                log.error("关闭连接XXXXXXXXXXXXXXXXXXX");
                this.mongoClient.close();
            }
        });
    }


}
