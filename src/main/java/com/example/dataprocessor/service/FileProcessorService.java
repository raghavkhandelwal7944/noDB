package com.example.dataprocessor.service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import com.example.dataprocessor.model.SalesData;
import com.example.dataprocessor.repository.SalesDataRepository;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.scheduling.annotation.Async;
import java.time.format.TextStyle;
import java.util.Locale;
import java.time.format.DateTimeFormatter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Optional;
import java.io.InputStream;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

@Service
@org.springframework.scheduling.annotation.EnableAsync
public class FileProcessorService {
    private static final Logger logger = LoggerFactory.getLogger(FileProcessorService.class);
    
    // Add these field declarations
    private final AtomicInteger totalProcessedRows = new AtomicInteger(0);
    private final AtomicInteger totalFailedRows = new AtomicInteger(0);
    
    // Replace logProcess with this method
    private void logProgress(int currentRow, int totalRows) {
        if (currentRow % 5000 == 0) {  // Changed from 500 to 5000 to reduce logging frequency
            logger.info("Processing progress: {} of {} rows ({} failed)", 
                currentRow, 
                totalRows,
                totalFailedRows.get());
        }
    }
    private static final int CHUNK_SIZE = 500;
    private static final int HEADER_SAMPLE_ROWS = 10;

    // Add missing field declarations
    private final AtomicInteger processedRowsCounter;
    private final AtomicInteger failedRowsCounter;
    
    @Autowired
    private SalesDataRepository salesDataRepository;

    @Autowired
    private ColumnGuessingService columnGuessingService;

    @Autowired
    private FileTrackerService fileTrackerService;

    // Define ExecutorService bean
    private final ExecutorService taskExecutor;

    // Add constructor to initialize counters and executor
    @Autowired
    public FileProcessorService(@Qualifier("taskExecutor") ExecutorService taskExecutor) {
        this.taskExecutor = taskExecutor;
        this.processedRowsCounter = new AtomicInteger(0);
        this.failedRowsCounter = new AtomicInteger(0);
    }

    @Async
    public void processFile(MultipartFile file, Long fileStatusId) throws IOException, CsvValidationException, InterruptedException, ExecutionException {
        // Reset counters at start
        totalProcessedRows.set(0);
        totalFailedRows.set(0);
        processedRowsCounter.set(0);
        failedRowsCounter.set(0);
        
        long startTime = System.currentTimeMillis();
        logger.info("Starting to process file: {} with ID: {}", file.getOriginalFilename(), fileStatusId);
        
        try {
            List<List<String>> allRows = new ArrayList<>();
            String fileExtension = getFileExtension(file.getOriginalFilename());

            boolean hasHeader = false;
            List<List<String>> rawRows = new ArrayList<>();

            if ("xlsx".equalsIgnoreCase(fileExtension)) {
                logger.info("Reading Excel file raw data.");
                rawRows = readExcelFileRaw(file);
            } else if ("csv".equalsIgnoreCase(fileExtension)) {
                logger.info("Reading CSV file raw data.");
                rawRows = readCsvFileRaw(file);
            } else {
                logger.error("Unsupported file type: {}", fileExtension);
                throw new IllegalArgumentException("Unsupported file type: " + fileExtension);
            }

            if (rawRows.isEmpty()) {
                logger.warn("No data found in file: {}", file.getOriginalFilename());
                fileTrackerService.updateFileStatus(fileStatusId, "COMPLETED", "No data found in file.");
                return;
            }

            List<String> firstRow = rawRows.get(0);
            if (columnGuessingService.isLikelyHeader(firstRow)) {
                hasHeader = true;
                allRows.addAll(rawRows.subList(1, rawRows.size()));
            } else {
                allRows.addAll(rawRows);
            }

            // Get column mapping
            Map<Integer, SalesColumn> columnMapping = columnGuessingService.guessColumns(
                allRows.subList(0, Math.min(HEADER_SAMPLE_ROWS, allRows.size())),
                hasHeader ? Optional.of(firstRow) : Optional.empty()
            );

            // Process in chunks
            for (int i = 0; i < allRows.size(); i += CHUNK_SIZE) {
                List<List<String>> chunk = allRows.subList(i, Math.min(i + CHUNK_SIZE, allRows.size()));
                final int chunkStartIndex = i;
                
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    List<SalesData> salesDataList = new ArrayList<>();
                    
                    for (List<String> row : chunk) {
                        try {
                            SalesData salesData = mapRowToSalesData(row, columnMapping);
                            if (salesData != null) {
                                salesDataList.add(salesData);
                                processedRowsCounter.incrementAndGet();
                            } else {
                                failedRowsCounter.incrementAndGet();
                            }
                            
                            logProgress(processedRowsCounter.get(), allRows.size());
                            
                        } catch (Exception e) {
                            failedRowsCounter.incrementAndGet();
                        }
                    }
                    
                    try {
                        salesDataRepository.saveAll(salesDataList);
                    } catch (Exception e) {
                        failedRowsCounter.addAndGet(salesDataList.size());
                        processedRowsCounter.addAndGet(-salesDataList.size());
                    }
                }, taskExecutor);
                
                future.get();
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            String durationMessage = String.format(
                "File processing completed in %d seconds. Processed %d rows, %d failed rows.",
                duration / 1000,
                processedRowsCounter.get(),
                failedRowsCounter.get()
            );
            
            fileTrackerService.updateFileStatus(fileStatusId, "COMPLETED", durationMessage);
            logger.info(durationMessage);
            
        } catch (Exception e) {
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            String errorMessage = String.format(
                "File processing failed after %d seconds. Error: %s",
                duration / 1000,
                e.getMessage()
            );
            
            fileTrackerService.updateFileStatus(fileStatusId, "FAILED", errorMessage);
            logger.error(errorMessage);
            throw e;
        }
    }

    private void processChunk(List<List<String>> chunk, int startIndex, Map<Integer, SalesColumn> columnMapping) {
        List<SalesData> salesDataList = new ArrayList<>();
        
        for (List<String> row : chunk) {
            try {
                SalesData salesData = mapRowToSalesData(row, columnMapping);
                if (salesData != null) {
                    salesDataList.add(salesData);
                    totalProcessedRows.incrementAndGet();
                    
                    // Log progress every 500 rows
                    int processed = totalProcessedRows.get();
                    if (processed % 500 == 0) {
                        logger.info("Processed {} rows", processed);
                    }
                } else {
                    totalFailedRows.incrementAndGet();
                    logger.warn("Failed to map row at index {}", startIndex + chunk.indexOf(row));
                }
            } catch (Exception e) {
                totalFailedRows.incrementAndGet();
                logger.error("Error processing row at index {}: {}", startIndex + chunk.indexOf(row), e.getMessage());
            }
        }
        
        try {
            salesDataRepository.saveAll(salesDataList);
        } catch (Exception e) {
            totalFailedRows.addAndGet(salesDataList.size());
            totalProcessedRows.addAndGet(-salesDataList.size());
            logger.error("Error saving chunk starting at index {}: {}", startIndex, e.getMessage());
        }
    }


    private List<List<String>> readExcelFileRaw(MultipartFile file) throws IOException {
        try (InputStream inputStream = file.getInputStream()) {
            Workbook workbook = WorkbookFactory.create(inputStream);
            Sheet sheet = workbook.getSheetAt(0);
            List<List<String>> rows = new ArrayList<>();
            
            for (Row row : sheet) {
                List<String> rowData = new ArrayList<>();
                for (Cell cell : row) {
                    String value = getCellValueAsString(cell);
                    rowData.add(value);
                }
                rows.add(rowData);
            }
            logger.debug("Read {} rows from Excel file.", rows.size());
            return rows;
        }
    }

    private List<List<String>> readCsvFileRaw(MultipartFile file) throws IOException, CsvValidationException {
        List<List<String>> rows = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()));
             CSVReader csvReader = new CSVReader(reader)) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                rows.add(Arrays.asList(line));
            }
        }
        logger.debug("Read {} rows from CSV file.", rows.size());
        return rows;
    }

    private SalesData mapRowToSalesData(List<String> row, Map<Integer, SalesColumn> columnMapping) {
        SalesData salesData = new SalesData();
        boolean isEmptyRow = true;
        try {
            for (Map.Entry<Integer, SalesColumn> entry : columnMapping.entrySet()) {
                Integer colIdx = entry.getKey();
                SalesColumn salesColumn = entry.getValue();

                if (colIdx < row.size()) {
                    String cellValue = row.get(colIdx);
                    if (cellValue != null && !cellValue.trim().isEmpty()) {
                        isEmptyRow = false;
                    }
                    setSalesDataField(salesData, salesColumn, cellValue);
                }
            }
            return isEmptyRow ? null : salesData;
        } catch (Exception e) {
            logger.error("Error mapping row to SalesData: {}", e.getMessage());
            return null;
        }
    }

    private void setSalesDataField(SalesData salesData, SalesColumn column, String value) {
        String trimmedValue = (value != null) ? value.trim() : null;

        try {
            switch (column) {
                case SEGMENT:
                    salesData.setSegment(trimmedValue);
                    break;
                case COUNTRY:
                    salesData.setCountry(trimmedValue);
                    break;
                case PRODUCT:
                    salesData.setProduct(trimmedValue);
                    break;
                case DISCOUNT_BAND:
                    salesData.setDiscountBand(trimmedValue);
                    break;
                case UNITS_SOLD:
                    BigDecimal unitsSold = parseBigDecimal(trimmedValue);
                    salesData.setUnitsSold(unitsSold);
                    break;
                case MANUFACTURING_PRICE:
                    BigDecimal manufacturingPrice = parseBigDecimal(trimmedValue);
                    salesData.setManufacturingPrice(manufacturingPrice);
                    break;
                case SALE_PRICE:
                    BigDecimal salePrice = parseBigDecimal(trimmedValue);
                    salesData.setSalePrice(salePrice);
                    break;
                case GROSS_SALES:
                    BigDecimal grossSales = parseBigDecimal(trimmedValue);
                    salesData.setGrossSales(grossSales);
                    break;
                case DISCOUNTS:
                    BigDecimal discounts = parseBigDecimal(trimmedValue);
                    salesData.setDiscounts(discounts);
                    break;
                case SALES:
                    BigDecimal sales = parseBigDecimal(trimmedValue);
                    salesData.setSales(sales);
                    break;
                case COGS:
                    BigDecimal cogs = parseBigDecimal(trimmedValue);
                    salesData.setCogs(cogs);
                    break;
                case PROFIT:
                    BigDecimal profit = parseBigDecimal(trimmedValue);
                    salesData.setProfit(profit);
                    break;
                case DATE:
                    LocalDate date = parseLocalDate(trimmedValue);
                    if (date != null) {
                        salesData.setDate(date);
                        salesData.setMonthNumber(date.getMonthValue());
                        salesData.setMonthName(date.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH));
                        salesData.setYear(date.getYear());
                    }
                    break;
            }
        } catch (Exception e) {
            logger.error("Error setting field {}: {}", column, e.getMessage());
        }
    }

    private BigDecimal parseBigDecimal(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            // Remove currency symbols, commas, parentheses, and any non-numeric characters
            // except for the leading minus sign and the decimal point.
            String cleanValue = value.trim()
                                   .replace("$", "")
                                   .replace(",", "")
                                   .replace("(", "")
                                   .replace(")", "");

            // Handle potential empty string after cleaning (e.g., if input was just "-")
            if (cleanValue.isEmpty() || cleanValue.equals("-")) {
                return BigDecimal.ZERO; // Treat empty or single hyphen as zero
            }
            
            return new BigDecimal(cleanValue);
        } catch (NumberFormatException e) {
            System.err.println("Error parsing BigDecimal from: " + value + ". Error: " + e.getMessage());
            return null;
        }
    }

    private Integer parseInteger(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            // Remove currency symbols, commas, parentheses, and any non-numeric characters
            // except for the leading minus sign.
            String cleanValue = value.trim()
                                   .replace("$", "")
                                   .replace(",", "")
                                   .replace("(", "")
                                   .replace(")", "");

            // Handle potential empty string after cleaning (e.g., if input was just "-")
            if (cleanValue.isEmpty() || cleanValue.equals("-")) {
                return 0; // Treat empty or single hyphen as zero
            }

            return Integer.parseInt(cleanValue);
        } catch (NumberFormatException e) {
            // Mimic pandas errors='coerce': return null if parsing fails
            return null;
        }
    }

    private LocalDate parseLocalDate(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        String trimmedValue = value.trim();

        // Try parsing with ISO_LOCAL_DATE_TIME first, as it handles 'YYYY-MM-DDTHH:mm'
        try {
            LocalDate date = LocalDate.parse(trimmedValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            logger.debug("    Value '{}' is a LocalDate with ISO_LOCAL_DATE_TIME format.", value);
            return date;
        } catch (DateTimeParseException ignored) {
            // Fallback to other formats if ISO_LOCAL_DATE_TIME fails
        }

        for (String format : ColumnGuessingService.getDateFormatStrings()) {
            try {
                LocalDate date = LocalDate.parse(trimmedValue, DateTimeFormatter.ofPattern(format));
                logger.debug("    Value '{}' is a LocalDate with format {}", value, format);
                return date;
            } catch (DateTimeParseException ignored) {
                // Try next format
            }
        }

        // Also try to parse as Excel numeric date
        try {
            double excelDate = Double.parseDouble(trimmedValue);
            // Check for a reasonable range for Excel dates (e.g., between 1900-01-01 and 2100-01-01)
            // Excel's epoch is 1900-01-01, which is day 1. Max reasonable date around 2070 is ~70000.
            if (excelDate > 0 && excelDate < 100000) { 
                // Convert Excel numeric date to Java LocalDate
                LocalDate date = LocalDate.of(1899, 12, 31).plusDays((long) excelDate);
                logger.debug("    Value '{}' is an Excel numeric date, parsed to {}.", value, date);
                return date;
            }
        } catch (NumberFormatException ignored) {
            // Not an Excel date
        }
        logger.error("Could not parse date '{}' with any specified format or as Excel numeric.", value);
        return null;
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) {
            return "";
        }
        try {
            switch (cell.getCellType()) {
                case STRING:
                    return cell.getStringCellValue();
                case NUMERIC:
                    if (DateUtil.isCellDateFormatted(cell)) {
                        return cell.getLocalDateTimeCellValue()
                                  .toLocalDate()
                                  .format(DateTimeFormatter.ISO_LOCAL_DATE);
                    }
                    // Avoid scientific notation
                    return new java.text.DecimalFormat("#.##########").format(cell.getNumericCellValue());
                case BOOLEAN:
                    return String.valueOf(cell.getBooleanCellValue());
                case FORMULA:
                    try {
                        return new java.text.DecimalFormat("#.##########").format(cell.getNumericCellValue());
                    } catch (Exception e) {
                        try {
                            return cell.getStringCellValue();
                        } catch (Exception ex) {
                            logger.error("Error evaluating formula: {}", ex.getMessage());
                            return "";
                        }
                    }
                default:
                    return "";
            }
        } catch (Exception e) {
            logger.error("Error getting cell value: {}", e.getMessage());
            return "";
        }
    }

    private String getFileExtension(String fileName) {
        if (fileName == null || fileName.lastIndexOf('.') == -1) {
            return "";
        }
        return fileName.substring(fileName.lastIndexOf('.') + 1);
    }
}