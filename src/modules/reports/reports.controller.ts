import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { ReportsService } from './reports.service';
import { CreateReportDto } from './dto/create-report.dto';
import { UpdateReportDto } from './dto/update-report.dto';

@Controller('reports')
export class ReportsController {
  constructor(private readonly service: ReportsService) {}

  @Post()
  async create(@Body() dto: CreateReportDto) {
    const data = await this.service.create(dto);
    return { message: 'Report created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'All reports retrieved', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return { message: 'Report retrieved', data };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateReportDto) {
    const data = await this.service.update(+id, dto);
    return { message: 'Report updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Report deleted successfully', data };
  }
}
