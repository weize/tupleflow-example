/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.ciir.tupleflow.example;

import edu.umass.ciir.tupleflow.example.types.Word;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author wkong
 */
@Verified
@InputClass(className = "edu.umass.ciir.tupleflow.example.types.Word", order = {"+word"})
public class WordCountWriter implements Processor<Word> {

    Word preWord;
    int count;
    BufferedWriter writer;

    public WordCountWriter(TupleFlowParameters parameters) throws IOException {
        Parameters p = parameters.getJSON();
        String filename = p.getAsString("output");
        writer = getWriter(filename);
        preWord = null;
    }

    @Override
    public void process(Word word) throws IOException {
        if (preWord == null) {
            preWord = word;
            return;
        }

        if (word.word.equals(preWord.word)) {
            preWord.count += word.count;
        } else {
            write();

            preWord = word;
        }
    }

    private void write() throws IOException {
        writer.write(preWord.word + "\t" + preWord.count);
        writer.newLine();
    }

    @Override
    public void close() throws IOException {
        if (preWord != null) {
            write();
        }
        writer.close();
    }

    public static BufferedWriter getWriter(File file) throws IOException {
        if (file.getName().endsWith(".gz")) {
            return getGzipWriter(file);
        } else {
            return getNormalWriter(file);
        }
    }

    private static BufferedWriter getGzipWriter(File file) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file))));
    }

    private static BufferedWriter getNormalWriter(File file) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        return writer;
    }

    public static BufferedWriter getNormalWriter(String fileName) throws IOException {
        return getNormalWriter(new File(fileName));
    }

    public static BufferedWriter getWriter(String fileName) throws IOException {
        return getWriter(new File(fileName));
    }

    public static BufferedWriter getGzipWriter(String filename) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(filename))));
    }

}
