package org.apache.flink.runtime.util;

import gurobi.GRB;
import gurobi.GRBException;
import gurobi.GRBModel;
import gurobi.GRBVar;
import org.apache.flink.types.TwoKeysMap;
import org.apache.flink.types.TwoKeysMultiMap;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class GRBUtils {
    public static String matrixToString(int[][] matrix) {
        StringBuilder out = new StringBuilder();
        for (int[] row : matrix) {
            for (int j = 0; j < matrix[0].length; j++) {
                out.append(row[j]).append("\t");
            }
            out.append("\n");
        }
        return out.toString();
    }

	public static String matrixToString(GRBModel model, GRBVar[][] varMatrix) throws GRBException {
    	if(!isSolved(model)) {
    		return "Solve the model first";
		}
        String out = "";
        double[][] valueMatrix = new double[0][0];

        try {
            valueMatrix = model.get(GRB.DoubleAttr.X, varMatrix);
        } catch (GRBException e) {
            e.printStackTrace();
        }

        out += matrixToString(valueMatrix);

        return out;
    }

	public static String matrixToString(double[][] matrix) {
        StringBuilder out = new StringBuilder();
        DecimalFormat df = new DecimalFormat("0.00");
        for (double[] row : matrix) {
            out.append(arrayToString(row));
            out.append("\n");
        }
        return out.toString();
    }

	public static String arrayToString(double[] array) {
        StringBuilder out = new StringBuilder();
        DecimalFormat df = new DecimalFormat("0.00");
        for (int j = 0; j < array.length; j++) {
            if (df.format(array[j]).matches("^(\\+|-)*0.00$")) {
                out.append("-\t\t");
            } else {
                out.append(df.format(array[j])).append("\t");
            }
        }
        return out.toString();
    }

    public static String arrayToString(GRBModel solvedModel, GRBVar[] varArray) throws GRBException {
		if(!isSolved(solvedModel)) {
			return "Solve the model first";
		}

        String out = "";

        double[] valueArray = new double[0];

        try {
            valueArray = solvedModel.get(GRB.DoubleAttr.X, varArray);
        } catch (GRBException e) {
            e.printStackTrace();
        }

        out += arrayToString(valueArray);

        return out;
    }

    public static <K,V> String mapToString(Map<K, V> map) {
    	StringBuilder out = new StringBuilder();
		for (Map.Entry<K, V> entry : map.entrySet()) {
			out.append("\n\t").append(entry.getKey().toString());
			out.append("\n\t\t").append(entry.getValue().toString());
		}
		return out.toString();
	}


	public static <K> String mapToString(GRBModel solvedModel, Map<K,GRBVar> map) throws GRBException {
		if(!isSolved(solvedModel)) {
			return "Solve the model first";
		}
    	Map<K, Double> valueMap = new HashMap<>();
		for (Map.Entry<K, GRBVar> entry : map.entrySet()) {
			valueMap.put(entry.getKey(), entry.getValue().get(GRB.DoubleAttr.X));
		}
		return mapToString(valueMap);
	}

	public static <K1, K2> String twoKeysMapToString(GRBModel solvedModel, TwoKeysMap<K1, K2, GRBVar> map) throws GRBException {
		if(!isSolved(solvedModel)) {
			return "Solve the model first";
		}
		TwoKeysMap<K1, K2, Double> valueMap = new TwoKeysMultiMap<>();
		for (TwoKeysMap.Entry<K1, K2, GRBVar> entry : map.entrySet()) {
			valueMap.put(entry.getKey1(), entry.getKey2(), entry.getValue().get(GRB.DoubleAttr.X));
		}
		return twoKeysMapToString(valueMap);
	}

	private static <K1, K2, V> String twoKeysMapToString(TwoKeysMap<K1,K2,V> map) {
		StringBuilder out = new StringBuilder();
		for (TwoKeysMap.Entry<K1, K2, V> entry : map.entrySet()) {
			out.append("\n\t").append(entry.getKey1().toString());
			out.append(" ").append(entry.getKey2().toString());
			out.append("\n\t\t").append(entry.getValue().toString());
		}
		return out.toString();
	}

	public static boolean isSolved(GRBModel model) throws GRBException {
    	return model.get(GRB.IntAttr.SolCount) > 0;
	}
}
