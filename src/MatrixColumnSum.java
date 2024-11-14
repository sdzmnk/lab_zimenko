import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.*;


public class MatrixColumnSum {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Scanner scanner = new Scanner(System.in);

        int rowsMatrix = getDimension(scanner, "рядків");
        int colsMatrix = getDimension(scanner, "стовпців");

        System.out.println("Розмір матриці: " + rowsMatrix + " x " + colsMatrix);

        int minMatrixValue = getMinValue(scanner);
        int maxMatrixValue = getMaxValue(scanner, minMatrixValue);


        int[][] matrix = generateMatrix(rowsMatrix, colsMatrix, minMatrixValue, maxMatrixValue);
        displayMatrix(matrix);

        System.out.println("\nПідхід Work Stealing...");
        long startTime = System.nanoTime();
        int[] workStealingResults = sumColumnsWorkStealing(matrix);
        long workStealingTime = System.nanoTime() - startTime;
        displayResults(workStealingResults, workStealingTime);

        System.out.println("\nПідхід Work Dealing...");
        startTime = System.nanoTime();
        int[] workDealingResults = sumColumnsWorkDealing(matrix);
        long workDealingTime = System.nanoTime() - startTime;
        displayResults(workDealingResults, workDealingTime);
    }

    private static int[][] generateMatrix(int rows, int cols, int min, int max) {
        Random rand = new Random();
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = rand.nextInt((max - min) + 1) + min;
            }
        }
        return matrix;
    }

    private static void displayMatrix(int[][] matrix) {
        System.out.println("Згенерована матриця:");
        for (int[] row : matrix) {
            for (int num : row) {
                System.out.printf("%4d", num);
            }
            System.out.println();
        }
    }

    private static int[] sumColumnsWorkStealing(int[][] matrix) throws InterruptedException, ExecutionException {
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        return forkJoinPool.invoke(new ColumnSumTask(matrix, 0, matrix[0].length));
    }

    private static int[] sumColumnsWorkDealing(int[][] matrix) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newFixedThreadPool(matrix[0].length);
        Future<Integer>[] results = new Future[matrix[0].length];
        int[] sums = new int[matrix[0].length];

        for (int col = 0; col < matrix[0].length; col++) {
            final int column = col;
            results[col] = executor.submit(() -> {
                int sum = 0;
                for (int[] ints : matrix) {
                    sum += ints[column];
                }
                return sum;
            });
        }

        for (int i = 0; i < results.length; i++) {
            sums[i] = results[i].get();
        }

        executor.shutdown();
        return sums;
    }

    private static void displayResults(int[] results, long timeInNs) {
        System.out.println("Сума стовпців: ");
        for (int sum : results) {
            System.out.printf("%4d", sum);
        }
        long timeInMs = TimeUnit.NANOSECONDS.toMillis(timeInNs);
        System.out.println("\nЧас виконання (ms): " + timeInMs);
    }

    public static int getMinValue(Scanner scanner) {
        int min;

        while (true) {
            System.out.print("Введіть мінімальне значення для елементів матриці: ");
            try {
                min = Integer.parseInt(scanner.nextLine());
                break;
            } catch (NumberFormatException e) {
                System.out.println("Помилка: Введіть ціле число.");
            }
        }

        return min;
    }

    public static int getMaxValue(Scanner scanner, int min) {
        int max;

        while (true) {
            System.out.print("Введіть максимальне значення для елементів матриці: ");
            try {
                max = Integer.parseInt(scanner.nextLine());

                if (max < min) {
                    System.out.println("Помилка: Максимальне значення повинно бути не меншим за мінімальне.");
                } else {
                    break;
                }

            } catch (NumberFormatException e) {
                System.out.println("Помилка: Введіть ціле число.");
            }
        }

        return max;
    }

    private static int getDimension(Scanner scanner, String dimensionName) {
        int dimension = 0;

        while (true) {
            System.out.print("Введіть кількість " + dimensionName + " матриці: ");
            try {
                dimension = Integer.parseInt(scanner.nextLine());

                if (dimension <= 0) {
                    System.out.println("Помилка: Кількість " + dimensionName + " повинна бути додатним числом.");
                } else if (dimension > 1000) {
                    System.out.println("Помилка: Кількість " + dimensionName + " занадто велика.");
                } else {
                    break;
                }

            } catch (NumberFormatException e) {
                System.out.println("Помилка: Введіть ціле число.");
            }
        }

        return dimension;
    }

    static class ColumnSumTask extends RecursiveTask<int[]> {
        private static final int THRESHOLD = 1;
        private final int[][] matrix;
        private final int start;
        private final int end;

        public ColumnSumTask(int[][] matrix, int start, int end) {
            this.matrix = matrix;
            this.start = start;
            this.end = end;
        }

        @Override
        protected int[] compute() {
            if ((end - start) <= THRESHOLD) {
                int[] result = new int[end - start];
                for (int i = start; i < end; i++) {
                    int sum = 0;
                    for (int[] ints : matrix) {
                        sum += ints[i];
                    }
                    result[i - start] = sum;
                }
                return result;
            } else {
                int mid = (start + end) / 2;
                ColumnSumTask leftTask = new ColumnSumTask(matrix, start, mid);
                ColumnSumTask rightTask = new ColumnSumTask(matrix, mid, end);
                invokeAll(leftTask, rightTask);
                int[] leftResult = leftTask.join();
                int[] rightResult = rightTask.join();
                int[] mergedResult = new int[leftResult.length + rightResult.length];
                System.arraycopy(leftResult, 0, mergedResult, 0, leftResult.length);
                System.arraycopy(rightResult, 0, mergedResult, leftResult.length, rightResult.length);
                return mergedResult;
            }
        }
    }
}
