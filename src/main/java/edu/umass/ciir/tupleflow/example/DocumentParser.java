/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.ciir.tupleflow.example;

import edu.umass.ciir.tupleflow.example.types.Word;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 * Read a file and emit all words
 *
 * @author wkong
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.FileName")
@OutputClass(className = "edu.umass.ciir.tupleflow.example.types.Word")
public class DocumentParser extends StandardStep<FileName, Word> {

    @Override
    public void process(FileName filename) throws IOException {
        BufferedReader reader = getReader(filename.filename);

        String line;
        while((line = reader.readLine()) != null) {
            line = line.toLowerCase();
            line = line.replaceFirst("[^0-9a-z]", " ");
            String [] words = line.split("\\s+");
            for(String word : words) {
                processor.process(new Word(word, 1));
            }
        }

    }

    public static BufferedReader getReader(String filename) throws IOException {
        return getReader(new File(filename));
    }

    public static BufferedReader getReader(File file) throws IOException {
        FileInputStream stream = new FileInputStream(file);
        if (file.getName().endsWith(".gz")) {
            return new BufferedReader(
                    new InputStreamReader(
                            new GZIPInputStream(stream)));
        } else {
            return new BufferedReader(
                    new InputStreamReader(stream));
        }
    }

}
