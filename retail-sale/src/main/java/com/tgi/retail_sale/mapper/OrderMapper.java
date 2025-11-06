package com.tgi.retail_sale.mapper;

import com.tgi.retail_sale.model.OrderRequest;
import com.tgi.retail_sale.model.OrderResponse;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;


@Mapper(componentModel = "spring")
public interface OrderMapper {

    OrderResponse toOrderResponse(OrderRequest orderRequest);
}
