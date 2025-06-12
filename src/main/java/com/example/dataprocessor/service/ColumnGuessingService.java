package com.example.dataprocessor.service;

import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class ColumnGuessingService {

    private static final Logger logger = LoggerFactory.getLogger(ColumnGuessingService.class);

    // Regex patterns for date and integer detection for robustness
    private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("^-?\\d+\\.?\\d*$");

    @SuppressWarnings("unused") // Suppress warning as it's used reflectively or in ways not caught by static analysis
    private static final String[] DATE_FORMATS = new String[] {
        "dd/MM/yyyy", // Primary format as specified by user
        "yyyy-MM-dd",
        "MM/dd/yyyy",
        "yyyy/MM/dd",
        "MM-dd-yyyy",
        "dd-MM-yyyy"
    };

    /**
     * Guesses the mapping of column index to SalesColumn based on sample data and an optional header row.
     * Prioritizes header matching if a header row is provided and recognizable.
     *
     * @param sampleRows A list of lists, where each inner list represents a row of string data from the file.
     * @param headerRow An Optional containing the header row as a list of strings, or empty if no header.
     * @return A map where the key is the 0-indexed column index from the file and the value is the guessed SalesColumn.
     */
    public Map<Integer, SalesColumn> guessColumns(List<List<String>> sampleRows, Optional<List<String>> headerRow) {
        logger.info("Starting column guessing with {} sample rows and header presence: {}", sampleRows.size(), headerRow.isPresent());
        if (headerRow.isPresent()) {
            logger.debug("Provided header row: {}", headerRow.get());
        }

        Map<Integer, SalesColumn> columnMapping = new HashMap<>();
        int numColumns = sampleRows.isEmpty() ? (headerRow.map(List::size).orElse(0)) : sampleRows.get(0).size();

        boolean[] columnUsed = new boolean[numColumns];

        if (headerRow.isPresent()) {
            List<String> headers = headerRow.get();
            for (int colIdx = 0; colIdx < Math.min(headers.size(), numColumns); colIdx++) {
                String headerName = headers.get(colIdx).trim();
                for (SalesColumn salesColumn : SalesColumn.values()) {
                    if (headerName.equalsIgnoreCase(salesColumn.getColumnName())) {
                        columnMapping.put(colIdx, salesColumn);
                        columnUsed[colIdx] = true;
                        logger.debug("Header match found: Column '{}' mapped to SalesColumn {}", headerName, salesColumn);
                        break; // Move to next header column
                    }
                }
            }
        }

        // Fallback or fill in remaining columns based on data types
        // Prioritize guessing based on specific data types first (Date, Integer, BigDecimal)
        for (SalesColumn salesColumn : SalesColumn.values()) {
            if (!columnMapping.containsValue(salesColumn)) { // Only guess if not already mapped by header
                logger.debug("Attempting to guess column for SalesColumn: {} (Data Type: {})", salesColumn.getColumnName(), salesColumn.getDataType().getSimpleName());
                if (salesColumn.getDataType() == LocalDate.class) {
                    guessAndAssign(sampleRows, salesColumn, columnMapping, columnUsed, this::isLocalDate);
                } else if (salesColumn.getDataType() == Integer.class) {
                    guessAndAssign(sampleRows, salesColumn, columnMapping, columnUsed, this::isInteger);
                } else if (salesColumn.getDataType() == BigDecimal.class) {
                    guessAndAssign(sampleRows, salesColumn, columnMapping, columnUsed, this::isBigDecimal);
                }
            }
        }

        // Fill in remaining columns with String type if not already mapped
        for (SalesColumn salesColumn : SalesColumn.values()) {
            if (!columnMapping.containsValue(salesColumn) && salesColumn.getDataType() == String.class) {
                logger.debug("Attempting to guess String column for SalesColumn: {}", salesColumn.getColumnName());
                guessAndAssign(sampleRows, salesColumn, columnMapping, columnUsed, this::isString);
            }
        }
        logger.info("Final guessed column mapping: {}", columnMapping);
        return columnMapping;
    }

    /**
     * Determines if a given row is likely a header row based on its content.
     * A row is considered a header if a significant portion of its cells are non-numeric and match expected column names.
     *
     * @param row The list of strings representing a row.
     * @return true if the row is likely a header, false otherwise.
     */
    public boolean isLikelyHeader(List<String> row) {
        logger.debug("Checking if row is likely a header: {}", row);
        if (row == null || row.isEmpty()) {
            return false;
        }
        int nonNumericCount = 0;
        int recognizedHeaderCount = 0;
        int totalCells = 0;

        for (String cellValue : row) {
            if (cellValue != null && !cellValue.trim().isEmpty()) {
                totalCells++;
                String trimmedValue = cellValue.trim();

                // Check if it's not a numeric or date value (likely a text header)
                if (!isInteger(trimmedValue) && !isBigDecimal(trimmedValue) && !isLocalDate(trimmedValue)) {
                    nonNumericCount++;
                }

                // Check if it matches any of our expected column names (case-insensitive)
                for (SalesColumn salesColumn : SalesColumn.values()) {
                    if (trimmedValue.equalsIgnoreCase(salesColumn.getColumnName())) {
                        recognizedHeaderCount++;
                        break;
                    }
                }
            }
        }

        boolean result = (totalCells > 0 && (double) nonNumericCount / totalCells > 0.5) || recognizedHeaderCount > (SalesColumn.values().length / 3); // At least 1/3 of columns are recognized headers
        logger.debug("isLikelyHeader result for row {}: {}", row, result);
        return result;
    }

    private void guessAndAssign(List<List<String>> sampleRows, SalesColumn targetColumn, Map<Integer, SalesColumn> columnMapping, boolean[] columnUsed, java.util.function.Function<String, Boolean> typeChecker) {
        int numColumns = sampleRows.isEmpty() ? 0 : sampleRows.get(0).size();
        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            if (!columnUsed[colIdx]) {
                boolean allMatch = true;
                // Check sample rows for data type consistency
                for (List<String> row : sampleRows) {
                    if (colIdx < row.size()) {
                        String cellValue = row.get(colIdx);
                        boolean isOfType = typeChecker.apply(cellValue);
                        logger.debug("  Checking sample value '{}' (colIdx: {}) for type {}: {}", cellValue, colIdx, targetColumn.getDataType().getSimpleName(), isOfType);
                        if (cellValue != null && !cellValue.trim().isEmpty() && !isOfType) {
                            allMatch = false;
                            break;
                        }
                    }
                }
                if (allMatch) {
                    columnMapping.put(colIdx, targetColumn);
                    columnUsed[colIdx] = true;
                    logger.debug("Assigned column {} (Index: {}) to SalesColumn {}", targetColumn.getColumnName(), colIdx, targetColumn);
                    return; // Found a match for this targetColumn, move to next targetColumn
                }
            }
        }
    }

    private boolean isLocalDate(String value) {
        logger.debug("  Checking isLocalDate for value: '{}'", value);
        if (value == null || value.trim().isEmpty()) {
            return false; // Empty string is not a valid date
        }
        String trimmedValue = value.trim();
        for (String format : DATE_FORMATS) {
            try {
                LocalDate.parse(trimmedValue, DateTimeFormatter.ofPattern(format));
                logger.debug("    Value '{}' is a LocalDate with format {}", value, format);
                return true;
            } catch (DateTimeParseException ignored) {
                // Try next format
            }
        }

        // Also try to parse as Excel numeric date
        try {
            double excelDate = Double.parseDouble(trimmedValue);
            if (excelDate > 0 && excelDate < 100000) { // Reasonable range for Excel dates
                logger.debug("    Value '{}' is an Excel numeric date.", value);
                return true;
            }
        } catch (NumberFormatException ignored) {
            // Not an Excel date
        }
        logger.debug("    Value '{}' is NOT a LocalDate.", value);
        return false;
    }

    private boolean isInteger(String value) {
        logger.debug("  Checking isInteger for value: '{}'", value);
        if (value == null || value.trim().isEmpty()) {
            return false; // Empty string is not a valid integer
        }
        String cleanValue = value.trim()
                               .replace("$", "")
                               .replace(",", "")
                               .replace("(", "")
                               .replace(")", "");

        if (cleanValue.isEmpty() || cleanValue.equals("-")) {
            logger.debug("    Cleaned value '{}' is empty or hyphen, NOT an Integer.", cleanValue);
            return false; // An empty string or a lone hyphen after cleaning is not an integer for guessing purposes
        }

        try {
            BigDecimal bd = new BigDecimal(cleanValue);
            boolean isWhole = bd.scale() <= 0 || bd.stripTrailingZeros().scale() <= 0; // Check if it's a whole number
            logger.debug("    Value '{}' (cleaned: '{}') is Integer: {}", value, cleanValue, isWhole);
            return isWhole;
        } catch (NumberFormatException e) {
            logger.debug("    Value '{}' (cleaned: '{}') is NOT an Integer. Error: {}", value, cleanValue, e.getMessage());
            return false;
        }
    }

    private boolean isBigDecimal(String value) {
        logger.debug("  Checking isBigDecimal for value: '{}'", value);
        if (value == null || value.trim().isEmpty()) {
            return false; // Empty string is not a valid decimal
        }
        String cleanValue = value.trim()
                               .replace("$", "")
                               .replace(",", "")
                               .replace("(", "")
                               .replace(")", "");

        if (cleanValue.isEmpty() || cleanValue.equals("-")) {
            logger.debug("    Cleaned value '{}' is empty or hyphen, NOT a BigDecimal.", cleanValue);
            return false; // An empty string or a lone hyphen after cleaning is not a BigDecimal for guessing purposes
        }

        try {
            new BigDecimal(cleanValue);
            logger.debug("    Value '{}' (cleaned: '{}') is BigDecimal: true", value, cleanValue);
            return true;
        } catch (NumberFormatException e) {
            logger.debug("    Value '{}' (cleaned: '{}') is NOT a BigDecimal. Error: {}", value, cleanValue, e.getMessage());
            return false;
        }
    }

    private boolean isString(String value) {
        logger.debug("  Checking isString for value: '{}'", value);
        return true; // Any value can be a string
    }
} 