package com.odps.bitmap;

import com.aliyun.odps.udf.UDF;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;

public class GenerateBitMap extends UDF {
    public String evaluate(Long s) throws IOException {
        RoaringBitmap mrb=new RoaringBitmap();
        mrb.add(s.intValue());
        ByteBuffer outbb=ByteBuffer.allocate(mrb.serializedSizeInBytes());
        mrb.serialize(new DataOutputStream(new OutputStream() {
            ByteBuffer mBB;
            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
            public void close() {}
            public void flush() {}
            @Override
            public void write(int b) {
                mBB.put((byte) b);
            }
            public void write(byte[] b) {mBB.put(b);}
            public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
        }.init(outbb)));
        return Base64.getEncoder().encodeToString(outbb.array());
    }
}
