package com.odps.bitmap;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Iterator;

public class bitmapDemo2 {

    public static class BitMapper extends MapperBase {

        Record key;
        Record value;
        @Override
        public void setup(TaskContext context) throws IOException {
            key = context.createMapOutputKeyRecord();
            value = context.createMapOutputValueRecord();
        }

        @Override
        public void map(long recordNum, Record record, TaskContext context)
                throws IOException
        {
            RoaringBitmap mrb=new RoaringBitmap();
            long AID=0;
            {
                {
                    {
                        {
                            AID=record.getBigint("id");
                            mrb.add((int) AID);
                            //获取key
                            key.set(new Object[] {record.getString("active_date")});

                        }
                    }
                }
            }
            ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
            mrb.serialize(new DataOutputStream(new OutputStream(){
                ByteBuffer mBB;
                OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
                public void close() {}
                public void flush() {}
                public void write(int b) {
                    mBB.put((byte) b);}
                public void write(byte[] b) {mBB.put(b);}
                public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
            }.init(outbb)));
            String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
            value.set(new Object[] {serializedstring});
            context.write(key, value);
        }
    }

    public static class BitReducer extends ReducerBase {
        private Record result = null;

        public void setup(TaskContext context) throws IOException {
            result = context.createOutputRecord();
        }

        public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
            long fcount = 0;
            RoaringBitmap rbm=new RoaringBitmap();
            while (values.hasNext())
            {
                Record val = values.next();
                ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode((String)val.get(0)));
                ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(newbb);
                RoaringBitmap p= new RoaringBitmap(irb);
                rbm.or(p);
            }
            ByteBuffer outbb = ByteBuffer.allocate(rbm.serializedSizeInBytes());
            rbm.serialize(new DataOutputStream(new OutputStream(){
                ByteBuffer mBB;
                OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
                public void close() {}
                public void flush() {}
                public void write(int b) {
                    mBB.put((byte) b);}
                public void write(byte[] b) {mBB.put(b);}
                public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
            }.init(outbb)));
            String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
            result.set(0, key.get(0));
            result.set(1, serializedstring);
            context.write(result);
        }
    }
    public static void main( String[] args ) throws OdpsException
    {

        System.out.println("begin.........");
        JobConf job = new JobConf();

        job.setMapperClass(BitMapper.class);
        job.setReducerClass(BitReducer.class);

        job.setMapOutputKeySchema(SchemaUtils.fromString("active_date:string"));
        job.setMapOutputValueSchema(SchemaUtils.fromString("id:string"));

        InputUtils.addTable(TableInfo.builder().tableName("bitmap_source").cols(new String[] {"id","active_date"}).build(), job);
//        +------------+-------------+
//        | id         | active_date |
//        +------------+-------------+
//        | 1          | 20190729    |
//        | 2          | 20190729    |
//        | 3          | 20190730    |
//        | 4          | 20190801    |
//        | 5          | 20190801    |
//        +------------+-------------+
        OutputUtils.addTable(TableInfo.builder().tableName("bitmap_target").build(), job);
//        +-------------+------------+
//        | active_date | bit_map    |
//        +-------------+------------+
//        20190729,OjAAAAEAAAAAAAEAEAAAAAEAAgA=3D
//        20190730,OjAAAAEAAAAAAAAAEAAAAAMA
//        20190801,OjAAAAEAAAAAAAEAEAAAAAQABQA=3D

        JobClient.runJob(job);
    }
}
