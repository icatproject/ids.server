package org.icatproject.ids.v3.models;

import org.icatproject.ids.v3.enums.ValueContainerType;
import org.icatproject.ids.exceptions.InternalException;

import jakarta.servlet.http.HttpServletRequest;
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

    private ValueContainer() {
        this(null, ValueContainerType.INVALID);
    }

    /**
     * Creates a ValueContainer of type int
     * @param value the value contained by the container
     */
    public ValueContainer(int value) {
        this(value, ValueContainerType.INT);
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
     * Creates a ValueContainer of type Request
     * @param value the value contained by the container
     */
    public ValueContainer(HttpServletRequest value) {
        this(value, ValueContainerType.REQUEST);
    }

    /**
     * Creates a ValueContainer of type Response
     * @param value the value contained by the container
     */
    public ValueContainer(Response value) {
        this(value, ValueContainerType.RESPONSE);
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
     * Tries to return the value of the type Request.
     * @return
     * @throws InternalException if the container has another type an exception will be thrown
     */
    public HttpServletRequest getRequest() throws InternalException {
        this.checkType(ValueContainerType.REQUEST);
        return (HttpServletRequest) this.value;
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

    


}