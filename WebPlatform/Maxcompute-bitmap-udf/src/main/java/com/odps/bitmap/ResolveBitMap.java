package com.odps.bitmap;

import com.aliyun.odps.udf.UDF;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;

public class ResolveBitMap extends UDF {
    public String evaluate(String val) throws IOException {
//        RoaringBitmap rbm=new RoaringBitmap();
//        ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode((String)val));
//        ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(newbb);
//        RoaringBitmap p= new RoaringBitmap(irb);
//        rbm.or(p);
//        ByteBuffer outbb=ByteBuffer.allocate(mrb.serializedSizeInBytes());
//        mrb.serialize(new DataOutputStream(new OutputStream() {
//            ByteBuffer mBB;
//            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
//            public void close() {}
//            public void flush() {}
//            @Override
//            public void write(int b) {
//                mBB.put((byte) b);
//            }
//            public void write(byte[] b) {mBB.put(b);}
//            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
//        }.init(outbb)));
//        return Base64.getEncoder().encodeToString(outbb.array());
        return null;
    }
}
