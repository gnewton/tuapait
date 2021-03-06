package ca.gnewton.tuapait;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

public class Serializer{

    public static final byte[] kSerializeRecord(Object obj){
	ByteArrayOutputStream bos=null;
	Output output=null;
	try{
	    byte[] bytes=null;
	    Kryo kryo = new Kryo();
	    bos = new ByteArrayOutputStream();
	    output = new Output(bos);   
	    kryo.writeObject(output, obj);
	    output.close();
	    bos.close();
	    bytes = bos.toByteArray();
	    return bytes;
	}
	catch(Throwable t){
	    t.printStackTrace();
	    return null;
	}finally{
	    try{
		if(output != null){
		    output.close();
		}
	    }
	    catch(Throwable t){
	    }
	    try{
		if(bos != null){
		    bos.close();
		}
	    }catch(Throwable t){
	    }
	}
    }

    public static final Object kDeserializeRecord(byte[] bytes, Class toClass){
	System.err.println("bytes: " + bytes.length);
	Kryo kryo = new Kryo();
	Object obj = null;
	ByteArrayInputStream bos = null;
	ObjectInputStream in = null;

	bos = new ByteArrayInputStream(bytes);
	Input input = new Input(bos);
	Object someObject = kryo.readObject(input, toClass);
	input.close();
	try{
	    bos.close();
	}catch(Throwable t){
	    t.printStackTrace();
	}
	return someObject;
    }



    public static final byte[] serializeRecord(Object obj){
	if(false){
	    return kSerializeRecord(obj);
	}
	byte[] bytes=null;
	ByteArrayOutputStream bos = new ByteArrayOutputStream();
	ObjectOutput out = null;
	try {
	    out = new ObjectOutputStream(bos);   
	    out.writeObject(obj);
	    bytes = bos.toByteArray();
	    out.close();
	    bos.close();
	} catch(Throwable t){
	    t.printStackTrace();
	}
	finally {
	    
	}
	return bytes;
    }

    public static final Object deserializeRecord(byte[] bytes, Class toClass){
	if(false){
	    return kDeserializeRecord(bytes, toClass);
	}
	Object obj = null;
	ByteArrayInputStream bos = null;
	ObjectInputStream in = null;
	try
	    {
		bos = new ByteArrayInputStream(bytes);
		in = new ObjectInputStream(bos);
		obj = in.readObject();
	    }catch(IOException i)
	    {
		i.printStackTrace();
	    }catch(ClassNotFoundException c)
	    {
		c.printStackTrace();
	    }
	finally{
	    try{
		if(in != null){
		    in.close();
		}
		if(bos != null){
		    bos.close();
		}
	    }catch(Throwable t){
		t.printStackTrace();
	    }
	}
	return obj;
    }



}
