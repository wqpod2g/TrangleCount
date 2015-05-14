package nju.iip;

/**
 * 驱动程序
 * @author mrpod2g
 *
 */
public class TriangleCountDriver {
	
	public static void main(String[] args) throws Exception {

		String[] setMatrixPath = {args[0],args[1]};
		GetMatrix.main(setMatrixPath);//构造初始矩阵（邻接表型式）
		
		String[] setMatrixMultiplyPath1 = {"output1","output2"}; 
		MatrixMultiply.main(setMatrixMultiplyPath1);//第一次矩阵相乘
		
		String[] setMatrixMultiplyPath2 = {"output2","output3"}; 
		MatrixMultiply2.main(setMatrixMultiplyPath2);//第二次矩阵相乘
	}

}
