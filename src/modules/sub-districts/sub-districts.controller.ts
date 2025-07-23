import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { SubDistrictsService } from './sub-districts.service';
import { CreateSubDistrictDto } from './dto/create-sub-district.dto';
import { UpdateSubDistrictDto } from './dto/update-sub-district.dto';

@Controller('sub-districts')
export class SubDistrictsController {
  constructor(private readonly service: SubDistrictsService) {}

  @Post()
  async create(@Body() dto: CreateSubDistrictDto) {
    const data = await this.service.create(dto);
    return { message: 'Sub-district created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'Data sub-districts successfully', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    if (!data) return { message: 'Sub-district not found', data: null };
    return { message: 'Sub-district retrieved successfully', data };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateSubDistrictDto) {
    const data = await this.service.update(+id, dto);
    return { message: 'Sub-district updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Sub-district deleted successfully', data };
  }
}
