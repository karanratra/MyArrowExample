package com.github.karanratra;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class MyArrowFileReader {

	public static void main(String[] args) throws Exception {

		
		String fileName = args[0];

		File arrowFile = validateFile(fileName, true);

		RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);

		FileInputStream fileInputStream = new FileInputStream(arrowFile);

		DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

		ArrowFileReader arrowFileReader = new ArrowFileReader(new SeekableReadChannel(fileInputStream.getChannel()),
				rootAllocator);

		VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();

		System.out.println("File size : " + arrowFile.length() + " schema is " + root.getSchema().toString());
		 
		 List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
		 System.out.println("Number of arrow blocks are " + arrowBlocks.size());
		 
		 for (int i = 0; i < arrowBlocks.size(); i++) {
			    ArrowBlock rbBlock = arrowBlocks.get(i);
			    
			    if (!arrowFileReader.loadRecordBatch(rbBlock)) {
	                throw new IOException("Expected to read record batch") ;   
	            }
			    System.out.println("\t["+i+"] ArrowBlock, offset: " + rbBlock.getOffset() +
	                    ", metadataLength: " + rbBlock.getMetadataLength()  +
	                    ", bodyLength " + rbBlock.getBodyLength()); 
			     
			    List<FieldVector> fieldVector = root.getFieldVectors();
			    System.out.println(fieldVector);
			    for(int j = 0; j < fieldVector.size(); j++){
			    	 Types.MinorType mt = fieldVector.get(j).getMinorType();  
			    	  switch(mt){ 
			    	  case STRUCT: showStructAccessor(fieldVector.get(j)); break;
			    	  case INT: showIntAccessor(fieldVector.get(j)); break;
			    	  case VARCHAR : showVarCharAccessor(fieldVector.get(j)); break;
			    	  default: throw new Exception(" MinorType " + mt);
			    	  }
			    	 
			    }
		 }
 
	}

	private static void showStructAccessor(FieldVector fx) throws Exception{
        StructVector structVector = ((StructVector) fx);
        List<FieldVector> fieldVector = structVector.getChildrenFromFields();
        for(int j = 0; j < fieldVector.size(); j++){
	    	 Types.MinorType mt = fieldVector.get(j).getMinorType();
	    	  switch(mt){
	    	  case INT: showIntAccessor(fieldVector.get(j)); break; 
	    	  case STRUCT: showStructAccessor(fieldVector.get(j)); break;
	    	  default: throw new Exception(" MinorType " + mt);
	    	  }
	    	 
	    }
//        for(int j = 0; j < intVector.getValueCount(); j++){
//            if(!intVector.isNull(j)){
//                int value = intVector.get(j);
//                System.out.println("\t\t intAccessor[" + j +"] " + value);
//                
//            } else {
//
//                System.out.println("\t\t intAccessor[" + j +"] : NULL ");
//            }
//        }
    }
	
	 private static void showIntAccessor(FieldVector fx){
	        IntVector intVector = ((IntVector) fx);
	        for(int j = 0; j < intVector.getValueCount(); j++){
	            if(!intVector.isNull(j)){
	                int value = intVector.get(j);
	                System.out.println("\t\t intAccessor[" + j +"] " + value);
	              
	            } else {
	              
	                System.out.println("\t\t intAccessor [" + j +"] : NULL ");
	            }
	        }
	    }
	 
	 private static void showVarCharAccessor(FieldVector fx){
	        VarCharVector varCharVector = ((VarCharVector) fx);
	        for(int j = 0; j < varCharVector.getValueCount(); j++) {
	            if(!varCharVector.isNull(j)){
	                byte[] value = varCharVector.get(j);
	                
	                System.out.println("\t\t intAccessor[" + j +"] " + new String(value));
	              
	            } else {
	              
	                System.out.println("\t\t intAccessor[" + j +"] : NULL ");
	            }
	        }
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
