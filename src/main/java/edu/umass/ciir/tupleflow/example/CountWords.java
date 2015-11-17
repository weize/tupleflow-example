/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.ciir.tupleflow.example;

import edu.umass.ciir.tupleflow.example.types.Word;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 *
 * @author wkong
 */
public class CountWords extends AppFunction {

    @Override
    public String getName() {
        return "count-words";
    }

    @Override
    public String getHelpString() {
        return "tupleflow-example count-words --input=<file>";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
        Job job = createJob(p); // a tupleflow job
        AppFunction.runTupleFlowJob(job, p, output);
    }

    private Job createJob(Parameters p) {
        Job job = new Job();

        job.add(getSplitStage(p));
        job.add(getParseStage(p));
        job.add(getWrite(p));

        job.connect("split", "parse", ConnectionAssignmentType.Each);
        job.connect("parse", "write", ConnectionAssignmentType.Combined);

        return job;
    }

    /**
     * get filenames
     *
     * @param p
     * @return
     */
    private Stage getSplitStage(Parameters p) {
        Stage stage = new Stage("split");

        stage.addOutput("filenames", new FileName.FilenameOrder());

        List<String> inputFiles = p.getAsList("input");

        for(String f : inputFiles) {
            System.out.println(f);
        }
        
        Parameters parameter = new Parameters();
        parameter.set("input", new ArrayList());
        for (String input : inputFiles) {
            parameter.getList("input").add(new File(input).getAbsolutePath());
            System.out.println(new File(input).getAbsolutePath());
        }

        stage.add(new Step(FileSource.class, p));
        stage.add(Utility.getSorter(new FileName.FilenameOrder()));
        stage.add(new OutputStep("filenames"));
        return stage;
    }

    private Stage getParseStage(Parameters p) {
        Stage stage = new Stage("parse");

        stage.addInput("filenames", new FileName.FilenameOrder());
        stage.addOutput("words", new Word.WordOrder());

        stage.add(new InputStep("filenames"));
        stage.add(new Step(DocumentParser.class, p));
        stage.add(Utility.getSorter(new Word.WordOrder()));
        stage.add(new OutputStep("words"));
        return stage;
    }

    private Stage getWrite(Parameters p) {
        Stage stage = new Stage("write");

        stage.addInput("words", new Word.WordOrder());

        stage.add(new InputStep("words"));
        stage.add(new Step(WordCountWriter.class, p));

        return stage;
    }

}
