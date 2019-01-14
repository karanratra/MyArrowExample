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
import org.apache.arrow.vector.BufferLayout;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.primitives.Bytes;

import io.netty.buffer.ArrowBuf;

public class MyArrowFileWriter {

	
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
	public static void writeData(int count, StructVector parent) {
		
		    ComplexWriter writer = new ComplexWriterImpl("root", parent);
		    //StructWriter rootWriter = writer.rootAsStruct();
		    StructWriter structWriter = writer.rootAsStruct().struct("struct");
		    structWriter.start();
		   // StructWriter structWriter = rootWriter.struct("struct");
		   // rootWriter.start();
		    for (int i = 0; i < count; i++) {
			   
			   
			    structWriter.setPosition(i);
//			    structWriter.start();
			    structWriter.integer("age").writeInt(i+31);
			    
			   
		    }
		    structWriter.end();
		    writer.setValueCount(10);
		    
		    /*
		    IntWriter intWriter = rootWriter.integer("int");
		   
		    for (int i = 0; i < count; i++) {
		      intWriter.setPosition(i);
		      intWriter.writeInt(i);
		   
		    }
		    writer.setValueCount(count);
		    rootWriter.end();
		    */
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
		
		
		
		/*
		// Show information about the schema and layout
		for (Field field : root.getSchema().getFields()) {
			FieldVector fieldVector = root.getVector(field.getName());
			showFieldLayout(field, fieldVector);
		}

		Field field = root.getSchema().getFields().get(0);
		StructVector vector = (StructVector) field.createVector(originalVectorAllocator);
		
		DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
		FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
		ArrowFileWriter arrowFileWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());
		
		// Write the data
		int batchSize = 100;
		int maxEntries = 478;
		arrowFileWriter.start();
		for (int i = 0; i < maxEntries;) {
			int itemsToProcess = Math.min(batchSize, maxEntries - i);
			//root.setRowCount(itemsToProcess);

			//Field field = root.getSchema().getFields().get(0);
			//FieldVector fieldVector = root.getVector(field.getName());
			// Assume by default here the root contains only one field vector which is
			// Struct
			
			
			//StructVector vector = (StructVector) fieldVector;
			vector.setInitialCapacity(itemsToProcess);
			vector.allocateNewSafe();	
			
			 for(FieldVector fieldVector : vector.getChildrenFromFields()) {
				 System.out.println(fieldVector);
				 IntVector intVector = (IntVector) fieldVector;
				  intVector.setInitialCapacity(itemsToProcess);
				  intVector.allocateNew();
				  for(int j = 0; i < itemsToProcess; i++){
			            intVector.setSafe(i, 1, i);
			        }
			        // how many are set
			        fieldVector.setValueCount(itemsToProcess);
			 }
			
			//writeData(itemsToProcess, vector, originalVectorAllocator);
			//writeFieldInt(fieldVector, i, batchSize);
			// write(vector.getChild("root"), arrowFile, fileOutputStream);
			 
			arrowFileWriter.writeBatch();
			i += itemsToProcess;
		}
		arrowFileWriter.end();
		arrowFileWriter.close();
		fileOutputStream.flush();
		fileOutputStream.close();
		*/
		
	}

	private static void write(FieldVector parent, File file, FileOutputStream outStream) throws IOException {
		 VectorSchemaRoot root = new VectorSchemaRoot(parent);
		 try ( ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, outStream.getChannel());) {
		     
		      arrowWriter.writeBatch();
		     
		    }
		
	}

	private static void writeFieldInt(FieldVector fieldVector, int from, int items){
        TinyIntVector  intVector = (TinyIntVector) fieldVector;
        intVector.setInitialCapacity(items);
        intVector.allocateNew();
        for(int i = 0; i < items; i++){
            intVector.setSafe(i, 1);
        }
        // how many are set
        fieldVector.setValueCount(items);
    }
	
	
	private static void writeData(int count, StructVector parent, BufferAllocator allocator) throws Exception {
		ComplexWriter writer = new ComplexWriterImpl("root", parent);
		StructWriter rootWriter = writer.rootAsStruct();
		IntWriter intWriter = rootWriter.integer("int");

		//VarCharWriter strWriter = rootWriter.varChar("utfWriter");
		for (int j = 0; j < count; j++) {
			intWriter.setPosition(j);
			intWriter.writeInt(j);
			//strWriter.setPosition(j);
			//byte[] bytes = ("KARAN_" + Integer.toString(j).getBytes()).getBytes();
			//ArrowBuf tempBuf = allocator.buffer(bytes.length);
			//tempBuf.setBytes(0, bytes);
			//strWriter.writeVarChar(0, bytes.length, tempBuf);
			//tempBuf.release();
		}
		
		intWriter.close();
		writer.setValueCount(count);
	
		writer.clear();
		
	}

	private static void showFieldLayout(Field field, FieldVector fieldVector) {
		TypeLayout typeLayout = TypeLayout.getTypeLayout(field.getType());
		List<BufferLayout.BufferType> vectorTypes = typeLayout.getBufferTypes();
		ArrowBuf[] vectorBuffers = new ArrowBuf[vectorTypes.size()];

		if (vectorTypes.size() != vectorBuffers.length) {
			throw new IllegalArgumentException("vector types and vector buffers are not the same size: "
					+ vectorTypes.size() + " != " + vectorBuffers.length);
		}

		System.out.println(" ----- [ " + field.toString() + " ] -------- ");
		System.out.println("FieldVector type: " + fieldVector.getClass().getCanonicalName());
		System.out.println("TypeLayout is " + typeLayout.toString() + " vectorSize is " + vectorTypes.size());
		for (int i = 0; i < vectorTypes.size(); i++) {
			System.out.println(" \t vector type entries [" + i + "] " + vectorTypes.get(i).toString());
		}
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

	private static File validateFile(String fileName, boolean exists) {

		if (fileName == null)
			throw new IllegalArgumentException("Missing the file " + fileName);

		File f = new File(fileName);
		if (exists && (!f.exists() || f.isDirectory())) {
			throw new IllegalArgumentException("File not found :: " + f.getAbsolutePath());
		}

		return f;
	}
}
