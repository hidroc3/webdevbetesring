import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { HydraulicOutputHecrasService } from './hydraulic-output-hecras.service';
import { CreateHydraulicOutputHecrasDto } from './dto/create-hydraulic-output-hecras.dto';
import { UpdateHydraulicOutputHecrasDto } from './dto/update-hydraulic-output-hecras.dto';

@Controller('hydraulic-output-hecras')
export class HydraulicOutputHecrasController {
  constructor(private readonly service: HydraulicOutputHecrasService) {}

  @Post()
  async create(@Body() dto: CreateHydraulicOutputHecrasDto) {
    const data = await this.service.create(dto);
    return { message: 'Hydraulic output (HEC-RAS) created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'List of hydraulic outputs (HEC-RAS)', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    if (!data)
      return { message: 'Hydraulic output (HEC-RAS) not found', data: null };
    return { message: 'Hydraulic output (HEC-RAS) retrieved', data };
  }

  @Patch(':id')
  async update(
    @Param('id') id: string,
    @Body() dto: UpdateHydraulicOutputHecrasDto,
  ) {
    const data = await this.service.update(+id, dto);
    return { message: 'Hydraulic output (HEC-RAS) updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Hydraulic output (HEC-RAS) deleted successfully', data };
  }
}
