import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { HydrologicOutputHechmsService } from './hydrologic-output-hechms.service';
import { CreateHydrologicOutputHechmsDto } from './dto/create-hydrologic-output-hechms.dto';
import { UpdateHydrologicOutputHechmsDto } from './dto/update-hydrologic-output-hechms.dto';

@Controller('hydrologic-output-hechms')
export class HydrologicOutputHechmsController {
  constructor(private readonly service: HydrologicOutputHechmsService) {}

  @Post()
  async create(@Body() dto: CreateHydrologicOutputHechmsDto) {
    const data = await this.service.create(dto);
    return {
      message: 'Hydrologic output (HEC-HMS) created successfully',
      data,
    };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'List of hydrologic outputs (HEC-HMS)', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    if (!data)
      return { message: 'Hydrologic output (HEC-HMS) not found', data: null };
    return { message: 'Hydrologic output (HEC-HMS) retrieved', data };
  }

  @Patch(':id')
  async update(
    @Param('id') id: string,
    @Body() dto: UpdateHydrologicOutputHechmsDto,
  ) {
    const data = await this.service.update(+id, dto);
    return {
      message: 'Hydrologic output (HEC-HMS) updated successfully',
      data,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return {
      message: 'Hydrologic output (HEC-HMS) deleted successfully',
      data,
    };
  }
}
