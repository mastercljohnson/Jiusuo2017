package xajiusuo.ilikebannanas;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.core.Instances;
import weka.core.OptionHandler;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.logging.Filter;

/**
 * Created by NS on 2018/1/5.
 */
public class wekastuff {
    protected Classifier m_Classifier = null;
    protected Filter m_Filter = null;
    protected String m_TrainingFile = null;
    protected Instances m_Training = null;
    protected Evaluation m_Evalutation= null;
    public wekastuff(){
        super();
    }

    public void setClassifier(String name, String[] options) throws Exception {
        m_Classifier = Classifier.forName(name, options);
    }

    public void setFilter(String name, String[] options) throws Exception{
        m_Filter = (Filter) Class.forName(name).newInstance();
        if (m_Filter instanceof OptionHandler)
            ((OptionHandler) m_Filter).setOptions(options);
    }

    public void setTraining(String name) throws Exception{
        m_TrainingFile = name;
        m_Training = new Instances(
                new BufferedReader(new FileReader(m_TrainingFile)));
        m_Training.setClassIndex(m_Training.numAttributes()-1);
    }

    public void execute() throws Exception{
        m_Filter.setInputFormat(m_Training);
        Instances filtered = Filter.useFilter(m_Training, m_Filter);

        m_Classifier.buildClassifier(filtered);

        m_Evalutation = new Evaluation(filtered);
        m_Evalutation.crossValidateModel(m_Classifier,filtered,10, m_Training.getRandomNumberGenerator(1));
    }

    public String toString bv
}
