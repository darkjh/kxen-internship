package com.kxen.han.projection.hadoop.writable;

import com.google.common.io.Files;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(JUnit4.class)
public class GiraphProjectionVertexValueTest {

    @Test
    public void testNoValueSerialization() throws IOException {
        long[] neighbors = {1l, 2l, 3l, 4l, 123456l};

        GiraphProjectionVertexValue testVertexValue =
                new GiraphProjectionVertexValue(10, neighbors);

        File dir = Files.createTempDir();
        File file = new File(dir, "vertex-value-serialization");

        OutputStream os = new FileOutputStream(file);
        DataOutputStream ods = new DataOutputStream(os);
        testVertexValue.write(ods);
        ods.close();

        InputStream is = new FileInputStream(file);
        DataInputStream ids = new DataInputStream(is);
        GiraphProjectionVertexValue deSer =
                GiraphProjectionVertexValue.newInstance();
        deSer.readFields(ids);
        is.close();

        file.delete();
        dir.delete();

        assertEquals(10, deSer.round);
        assertEquals(false, deSer.containsValue);
        for (int i = 0; i < neighbors.length; i++) {
            assertEquals(neighbors[i], deSer.neighbors[i]);
        }
    }

    @Test
    public void testWithValueSerialization() throws IOException {
        long[] neighbors = {1l, 2l, 3l, 4l, 123456l};
        long[] values = {1l, 2l, 3l, 4l, 2222222l};

        GiraphProjectionVertexValue testVertexValue =
                new GiraphProjectionVertexValue(neighbors,
                        values, neighbors.length);

        File dir = Files.createTempDir();
        File file = new File(dir, "vertex-value-serialization");

        OutputStream os = new FileOutputStream(file);
        DataOutputStream ods = new DataOutputStream(os);
        testVertexValue.write(ods);
        ods.close();

        InputStream is = new FileInputStream(file);
        DataInputStream ids = new DataInputStream(is);
        GiraphProjectionVertexValue deSer =
                GiraphProjectionVertexValue.newInstance();
        deSer.readFields(ids);
        is.close();

        file.delete();
        dir.delete();

        assertEquals(-1, deSer.round);
        assertEquals(true, deSer.containsValue);
        for (int i = 0; i < neighbors.length; i++) {
            assertEquals(neighbors[i], deSer.neighbors[i]);
            assertEquals(values[i], deSer.values[i]);
        }

        deSer.values[0] = 1111l;
        assertFalse(deSer.values[0] == testVertexValue.values[0]);
    }
}
