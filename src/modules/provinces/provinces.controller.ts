import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { ProvincesService } from './provinces.service';
import { CreateProvinceDto } from './dto/create-province.dto';
import { UpdateProvinceDto } from './dto/update-province.dto';

@Controller('provinces')
export class ProvincesController {
  constructor(private readonly service: ProvincesService) {}

  @Post()
  async create(@Body() dto: CreateProvinceDto) {
    const data = await this.service.create(dto);
    return { message: 'Province created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'Data provinces successfully', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    if (!data) return { message: 'Province not found', data: null };
    return { message: 'Province retrieved successfully', data };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateProvinceDto) {
    const data = await this.service.update(+id, dto);
    return { message: 'Province updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Province deleted successfully', data };
  }
}
