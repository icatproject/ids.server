package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.junit.Test;

import org.icatproject.ids.enums.ValueContainerType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.helpers.ValueContainer;

public class TestValueContainer {

    @Test
    public void testInvalidValueContainer() throws Exception {

        var vc = ValueContainer.getInvalid();
        assertFalse(vc.isVoid());
        assertTrue(vc.getType() == ValueContainerType.INVALID);
    }

    @Test
    public void testVoidValueContainer() throws Exception {

        var vc = ValueContainer.getVoid();
        assertTrue(vc.isVoid());
        assertTrue(vc.getType() == ValueContainerType.VOID);
        assertFalse(vc.isInvalid());
    }

    @Test(expected = InternalException.class)
    public void testIntValueContainer() throws Exception {

        var vc = new ValueContainer(1);
        assertFalse(vc.isVoid());
        assertFalse(vc.isInvalid());
        assertFalse(vc.isNull());

        assertEquals(vc.getInt(), 1);

        vc.getLong();
    }

    @Test(expected = InternalException.class)
    public void testLongValueContainer() throws Exception {

        var vc = new ValueContainer(1L);
        assertFalse(vc.isVoid());
        assertFalse(vc.isInvalid());
        assertFalse(vc.isNull());

        assertEquals(vc.getLong(), 1L);

        vc.getInt();
    }

    @Test(expected = InternalException.class)
    public void testBoolValueContainer() throws Exception {

        var vc = new ValueContainer(true);
        assertFalse(vc.isVoid());
        assertFalse(vc.isInvalid());
        assertFalse(vc.isNull());

        assertEquals(vc.getBool(), true);

        vc.getString();
    }

    @Test(expected = InternalException.class)
    public void testStringValueContainer() throws Exception {

        var vc = new ValueContainer("test");
        assertFalse(vc.isVoid());
        assertFalse(vc.isInvalid());
        assertFalse(vc.isNull());

        assertEquals(vc.getString(), "test");

        vc.getBool();
    }

    @Test(expected = InternalException.class)
    public void testInputStreamValueContainer() throws Exception {

        String s = "test InputStream";
        var inputStream = new ByteArrayInputStream(s.getBytes());
        var vc = new ValueContainer(inputStream);
        assertFalse(vc.isVoid());
        assertFalse(vc.isInvalid());
        assertFalse(vc.isNull());

        assertEquals(vc.getInputStream(), inputStream);

        var ISReader = new InputStreamReader(vc.getInputStream(),
                StandardCharsets.UTF_8);
        var BReader = new BufferedReader(ISReader);
        String textFromStream = BReader.lines().collect(Collectors.joining());
        BReader.close();
        assertEquals(textFromStream, s);

        vc.getString();
    }

}