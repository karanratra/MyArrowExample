package com.github.karanratra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

public class MyArrowFileWriterWorking {

	private static File validateFile(String fileName, boolean exists) {

		if (fileName == null)
			throw new IllegalArgumentException("Missing the file " + fileName);

		File f = new File(fileName);
		if (exists && (!f.exists() || f.isDirectory())) {
			throw new IllegalArgumentException("File not found :: " + f.getAbsolutePath());
		}

		return f;
	}
	
	private static Schema makeSchema() {
		List<Field> fields = new ArrayList<>();
		Field age = new Field("age", new FieldType(true, new ArrowType.Int(32, true), null), null);
		Field name = new Field("name", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null);

		fields.add(age);
		fields.add(name);
		
		
		//return new Schema(fields);

		Field myStructField = new Field("my_struct", new FieldType(true, ArrowType.Struct.INSTANCE, null), fields);
		return new Schema(Collections.unmodifiableList(Arrays.asList(myStructField)));
	}
	

	public static void writeMyData(int count, StructVector parent,BufferAllocator allocator ) throws Exception {
		ComplexWriter writer = new ComplexWriterImpl("root", parent);
		 StructWriter rootWriter = writer.rootAsStruct();
		 IntWriter intWtiter = rootWriter.integer("age");
		 VarCharWriter charWriter = rootWriter.varChar("name");	 
		 rootWriter.start();
		   for (int i = 0; i < count; i++) {
			   rootWriter.setPosition(i);
			   charWriter.setPosition(i);
			   intWtiter.writeInt(51+i);
			   byte[] bytes = "karan".getBytes(StandardCharsets.UTF_8);
			   ArrowBuf tempBuf = allocator.buffer(bytes.length);
			   tempBuf.setBytes(0, bytes);
			   charWriter.writeVarChar(0, bytes.length, tempBuf);
			   
		   }
		  
		   rootWriter.end();
		   writer.setValueCount(count);
	}
	
	  /**
	   * Writes the contents of parents to file. If outStream is non-null, also writes it
	   * to outStream in the streaming serialized format.
	   */
	 public static void writeNew(FieldVector parent, File file, OutputStream outStream) throws IOException {
	    VectorSchemaRoot root = new VectorSchemaRoot(parent);

	    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
	         ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel());) {
	   
	      arrowWriter.start();
	      arrowWriter.writeBatch();
	      arrowWriter.end();
	    }

	  }
	 
	public static void main(String[] args) throws Exception {

		System.out.println("Number of arguments : " + args.length);
		String fileName = args[0];
		System.out.println("File Name :: " + fileName);

		File arrowFile = validateFile(fileName, false);

		Schema schema = makeSchema();

		BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
 		BufferAllocator originalVectorAllocator =
		           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
		
		 
		VectorSchemaRoot root = VectorSchemaRoot.create(schema, originalVectorAllocator);
		FieldVector fieldVector = root.getFieldVectors().get(0);
		StructVector parent = (StructVector) fieldVector;
		//writeData(10, parent); 
		writeMyData(100, parent,allocator);
		writeNew(parent.getChild("root"), arrowFile, new ByteArrayOutputStream());
		
	}
	
}
