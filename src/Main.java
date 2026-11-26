import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Л/р 9. Синхронное и асинхронное копирование средствами NIO.
 */
public class Main {
    public static void main(String[] args) {
        if (args.length != 0 && args.length != 2) {
            System.err.println("Укажите два исходных файла или запустите без аргументов.");
            return;
        }
        try {
            Path first = Path.of(args.length == 2 ? args[0] : "file1.txt");
            Path second = Path.of(args.length == 2 ? args[1] : "file2.txt");
            if (args.length == 0) {
                createTestFile(first, 2_000_000);
                createTestFile(second, 2_000_000);
            }
            Path seq1 = Path.of("copy_seq_1.txt"), seq2 = Path.of("copy_seq_2.txt");
            Path async1 = Path.of("copy_async_1.txt"), async2 = Path.of("copy_async_2.txt");
            for (Path source : new Path[]{first, second}) {
                for (Path target : new Path[]{seq1, seq2, async1, async2}) {
                    NioFileCopier.checkDifferentFiles(source, target);
                }
            }
            System.out.println("Синхронное копирование (FileChannel):");
            long start = System.nanoTime();
            NioFileCopier.copySync(first, seq1);
            System.out.println("Скопирован: " + first + " -> " + seq1);
            NioFileCopier.copySync(second, seq2);
            System.out.println("Скопирован: " + second + " -> " + seq2);
            long syncTime = System.nanoTime() - start;

            System.out.println("\nАсинхронное копирование (AsynchronousFileChannel):");
            start = System.nanoTime();
            // Запускаем обе операции до ожидания результатов.
            CompletableFuture<Void> a = report(NioFileCopier.copyAsync(first, async1), first, async1);
            CompletableFuture<Void> b = report(NioFileCopier.copyAsync(second, async2), second, async2);
            CompletableFuture.allOf(a, b).join();
            long asyncTime = System.nanoTime() - start;
            System.out.printf(Locale.ROOT, "\nСинхронно:  %.3f мс%n", syncTime / 1_000_000.0);
            System.out.printf(Locale.ROOT, "Асинхронно: %.3f мс%n", asyncTime / 1_000_000.0);
            System.out.println(asyncTime < syncTime
                    ? "В этом запуске асинхронное копирование быстрее."
                    : "В этом запуске синхронное копирование быстрее или равно по времени.");
            // Проверку выполняем вне измеряемого интервала.
            for (Path[] pair : new Path[][]{{first, seq1}, {second, seq2}, {first, async1}, {second, async2}}) {
                if (Files.mismatch(pair[0], pair[1]) != -1)
                    throw new IOException("Содержимое копии отличается: " + pair[1]);
            }
            System.out.println("Проверка: все четыре копии побайтно совпадают с исходниками.");
        } catch (Exception ex) {
            Throwable cause = ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
            System.err.println("Ошибка: " + cause.getMessage());
        }
    }

    private static CompletableFuture<Void> report(CompletableFuture<Void> task, Path from, Path to) {
        return task.whenComplete((unused, error) -> {
            if (error == null) System.out.println("Скопирован: " + from + " -> " + to);
            else System.err.println("Не удалось скопировать " + from + ": " + error.getMessage());
        });
    }

    private static void createTestFile(Path path, int lines) throws IOException {
        if (Files.exists(path)) return;
        System.out.println("Создаётся тестовый файл: " + path);
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (int i = 1; i <= lines; i++) {
                writer.write("Строка номер " + i + " текстового файла " + path.getFileName());
                writer.newLine();
            }
        }
    }
}
