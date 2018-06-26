package com.dkl.leanring.spark.kafka;

public class Fly {
	// 让数组内的每一个数与它所处位置之后的数交换来保证数组的良好随机效果  
	// 而不是每一个数与位置前或后的数交换，以防造成重复，影响随机效果  
	public void shuffle(int[] a) {
		int N = a.length;
		for (int i = 0; i < N; i++) {
			int r = i + StdRandom.uniform(N - i);
			int temp = a[i];
			a[i] = a[r];
			a[r] = temp;
		}
	}

	public void ShuffleTest(int M, int N) {
		int[] a = new int[M];
		int[][] b = new int[M][M];
		for (int i = 0; i < N; i++) {
			for (int h = 0; h < M; h++) {
				a[h] = h;
			}
			shuffle(a);
			for (int j = 0; j < M; j++) {
				for (int k = 0; k < M; k++) {
					if (j == a[k])
						b[j][k]++;
				}
			}
		}
		for (int i = 0; i < M; i++)
			for (int j = 0; j < M; j++) {
				System.out.print(b[i][j] + " ");
				if (j == M - 1)
					System.out.println();
			}
	}

	public static void main(String[] args) {
		new Fly().ShuffleTest(10, 20);
	}
}
