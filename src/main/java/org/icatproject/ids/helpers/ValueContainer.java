package org.icatproject.ids.helpers;

import java.io.InputStream;

import org.icatproject.ids.enums.ValueContainerType;
import org.icatproject.ids.exceptions.InternalException;

import jakarta.ws.rs.core.Response;

/**
 * This class provides a container vor carrying different types of values
 */
public class ValueContainer {

    private Object value;
    private ValueContainerType type;

    private ValueContainer(Object value, ValueContainerType type) {
        this.value = value;
        this.type = type;
    }

    /**
     * checks if the type of the contained value is the same as the typeToCheck
     * @param typeToCheck the type to be checked if the contained value is of
     * @throws InternalException if the types don't match an exception is thrown
     */
    private void checkType(ValueContainerType typeToCheck) throws InternalException {
        if(this.type != typeToCheck) throw new InternalException("This ValueContainer ist not of the needed type " + typeToCheck + " its type is " + this.type + ".");
    }

    public static ValueContainer getInvalid() {
        return new ValueContainer();
    }

    public static ValueContainer getVoid() {
        return new ValueContainer((Void) null);
    }

    private ValueContainer() {
        this(null, ValueContainerType.INVALID);
    }

    private ValueContainer(Void value) {
        this(null, ValueContainerType.VOID);
    }

    /**
     * Creates a ValueContainer of type int
     * @param value the value contained by the container
     */
    public ValueContainer(int value) {
        this(value, ValueContainerType.INT);
    }

    /**
     * Creates a ValueContainer of type long
     * @param value the value contained by the container
     */
    public ValueContainer(long value) {
        this(value, ValueContainerType.LONG);
    }

    /**
     * Creates a ValueContainer of type String
     * @param value the value contained by the container
     */
    public ValueContainer(String value) {
        this(value, ValueContainerType.STRING);
    }

    /**
     * Creates a ValueContainer of type boolean
     * @param value the value contained by the container
     */
    public ValueContainer(boolean value) {
        this(value, ValueContainerType.BOOL);
    }

    /**
     * Creates a ValueContainer of type Response
     * @param value the value contained by the container
     */
    public ValueContainer(Response value) {
        this(value, ValueContainerType.RESPONSE);
    } 
    
    /**
     * Creates a ValueContainer of type InputStream
     * @param value the value contained by the container
     */
    public ValueContainer(InputStream value) {
        this(value, ValueContainerType.INPUTSTREAM);
    }

    /**
     * Informs about the type of the contained value
     * @return
     */
    public ValueContainerType getType() {
        return this.type;
    }

    /**
     * Tries to return the value of the type int.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public int getInt() throws InternalException {
        this.checkType(ValueContainerType.INT);
        return (int) this.value;
    }

    /**
     * Tries to return the value of the type long.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public long getLong() throws InternalException {
        this.checkType(ValueContainerType.LONG);
        return (long) this.value;
    }

    /**
     * Tries to return the value of the type boolean.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public boolean getBool() throws InternalException {
        this.checkType(ValueContainerType.BOOL);
        return (boolean) this.value;
    }

    /**
     * Tries to return the value of the type String.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public String getString() throws InternalException {
        this.checkType(ValueContainerType.STRING);
        return (String) this.value;
    }

    /**
     * Tries to return the value of the type Response.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public Response getResponse() throws InternalException {
        this.checkType(ValueContainerType.RESPONSE);
        return (Response) this.value;
    }

    /**
     * Tries to return the value of the type InputStream.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public InputStream getInputStream() throws InternalException {
        this.checkType(ValueContainerType.INPUTSTREAM);
        return (InputStream) this.value;
    }

    @Override
    public String toString() {
        switch(this.type) {
            case INVALID: return ""+ValueContainerType.INVALID; 
            case VOID: return ""+ValueContainerType.VOID;
            case INT: return ""+this.value;
            case LONG: return ""+this.value;
            case BOOL: return ((boolean)this.value ? "true" : "false"); 
            case STRING: return (String)this.value; 
            case RESPONSE: return "Response " + ((Response) this.value).toString();
            case INPUTSTREAM: return "An InputStream which will be printed here to prevent it from closing (and maybe it is too long).";
            default:
                throw new RuntimeException("Doesn't know how to make a String vom ValueContainer of type " + this.type + ". Please implement a new case is ValueContainer.toString().");
        }
    }

    public boolean isNull() { return this.isInvalid() && this.value == null; }

    public boolean isInvalid() { return this.type == ValueContainerType.INVALID; }

    public boolean isVoid() { return this.type == ValueContainerType.VOID; }

    


}
