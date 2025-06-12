package com.example.dataprocessor.service;

import java.math.BigDecimal;
import java.time.LocalDate;

public enum SalesColumn {
    SEGMENT("Segment", String.class),
    COUNTRY("Country", String.class),
    PRODUCT("Product", String.class),
    DISCOUNT_BAND("Discount Band", String.class),
    UNITS_SOLD("Units Sold", BigDecimal.class),
    MANUFACTURING_PRICE("Manufacturing Price", BigDecimal.class),
    SALE_PRICE("Sale Price", BigDecimal.class),
    GROSS_SALES("Gross Sales", BigDecimal.class),
    DISCOUNTS("Discounts", BigDecimal.class),
    SALES("Sales", BigDecimal.class),
    COGS("COGS", BigDecimal.class),
    PROFIT("Profit", BigDecimal.class),
    DATE("Date", LocalDate.class),
    MONTH_NUMBER("Month Number", Integer.class),
    MONTH_NAME("Month Name", String.class),
    YEAR("Year", Integer.class);

    private final String columnName;
    private final Class<?> dataType;

    SalesColumn(String columnName, Class<?> dataType) {
        this.columnName = columnName;
        this.dataType = dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public Class<?> getDataType() {
        return dataType;
    }
} 