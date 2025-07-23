import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { VehicleTrackingLogsService } from './vehicle-tracking-logs.service';
import { CreateVehicleTrackingLogDto } from './dto/create-vehicle-tracking-log.dto';
import { UpdateVehicleTrackingLogDto } from './dto/update-vehicle-tracking-log.dto';

@Controller('vehicle-tracking-logs')
export class VehicleTrackingLogsController {
  constructor(private readonly service: VehicleTrackingLogsService) {}

  @Post()
  async create(@Body() dto: CreateVehicleTrackingLogDto) {
    const data = await this.service.create(dto);
    return { message: 'Vehicle tracking log created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'List of vehicle tracking logs', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    if (!data) return { message: 'Vehicle tracking log not found', data: null };
    return { message: 'Vehicle tracking log retrieved', data };
  }

  @Patch(':id')
  async update(
    @Param('id') id: string,
    @Body() dto: UpdateVehicleTrackingLogDto,
  ) {
    const data = await this.service.update(+id, dto);
    return { message: 'Vehicle tracking log updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Vehicle tracking log deleted successfully', data };
  }
}
