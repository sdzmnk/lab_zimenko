import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class LargeFilesFinder {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Введіть шлях до директорії: ");
        String directoryPath = scanner.nextLine();

        System.out.print("Введіть мінімальний розмір файлу в байтах (наприклад, 1048576 для 1МБ): ");
        long minFileSize = scanner.nextLong();

        scanner.close();

        System.out.println("\nВикористання Work Dealing:");
        long startTimeDealing = System.nanoTime();
        int countDealing = findLargeFilesWorkDealing(directoryPath, minFileSize);
        long endTimeDealing = System.nanoTime();
        System.out.println("Кількість файлів більших за " + minFileSize + " байтів: " + countDealing);
        System.out.println("Час виконання (Work Dealing): " + (endTimeDealing - startTimeDealing) / 1_000_000 + " мс");

        System.out.println("\nВикористання Work Stealing:");
        long startTimeStealing = System.nanoTime();
        int countStealing = findLargeFilesWorkStealing(directoryPath, minFileSize);
        long endTimeStealing = System.nanoTime();
        System.out.println("Кількість файлів більших за " + minFileSize + " байтів: " + countStealing);
        System.out.println("Час виконання (Work Stealing): " + (endTimeStealing - startTimeStealing) / 1_000_000 + " мс");


    }

    private static int findLargeFilesWorkStealing(String directoryPath, long minFileSize) throws InterruptedException, ExecutionException {
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        File directory = new File(directoryPath);
        List<File> files = listFiles(directory);
        FileCounterTask task = new FileCounterTask(files, minFileSize);
        int count = forkJoinPool.invoke(task);
        forkJoinPool.shutdown();
        return count;
    }


    private static int findLargeFilesWorkDealing(String directoryPath, long minFileSize) throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        File directory = new File(directoryPath);
        List<File> files = listFiles(directory);

        List<Future<Integer>> futures = new ArrayList<>();

        for (int i = 0; i < files.size(); i += 5) {
            List<File> batch = files.subList(i, Math.min(i + 5, files.size()));
            Future<Integer> future = executorService.submit(() -> {
                int localCount = 0;
                for (File file : batch) {
                    if (file.isFile() && file.length() > minFileSize) {
                        localCount++;
                    }
                }
                return localCount;
            });
            futures.add(future);
        }

        int count = 0;
        for (Future<Integer> future : futures) {
            count += future.get();
        }

        executorService.shutdown();
        return count;
    }

    private static List<File> listFiles(File directory) {
        List<File> files = new ArrayList<>();
        File[] directoryFiles = directory.listFiles();
        if (directoryFiles != null) {
            for (File file : directoryFiles) {
                if (file.isFile()) {
                    files.add(file);
                }
            }
        }
        return files;
    }

    private static class FileCounterTask extends RecursiveTask<Integer> {
        private static final int BATCH_SIZE = 5;
        private final List<File> files;
        private final long minFileSize;

        public FileCounterTask(List<File> files, long minFileSize) {
            this.files = files;
            this.minFileSize = minFileSize;
        }

        @Override
        protected Integer compute() {
            if (files.size() <= BATCH_SIZE) {
                int count = 0;
                for (File file : files) {
                    if (file.isFile() && file.length() > minFileSize) {
                        count++;
                    }
                }
                return count;
            } else {

                int mid = files.size() / 2;
                FileCounterTask subtask1 = new FileCounterTask(files.subList(0, mid), minFileSize);
                FileCounterTask subtask2 = new FileCounterTask(files.subList(mid, files.size()), minFileSize);

                subtask1.fork();
                int count2 = subtask2.compute();
                int count1 = subtask1.join();

                return count1 + count2;
            }
        }
    }
}
