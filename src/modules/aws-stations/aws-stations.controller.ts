import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { AwsStationsService } from './aws-stations.service';
import { CreateAwsStationDto } from './dto/create-aws-station.dto';
import { UpdateAwsStationDto } from './dto/update-aws-station.dto';

@Controller('aws-stations')
export class AwsStationsController {
  constructor(private readonly service: AwsStationsService) {}

  @Post()
  async create(@Body() dto: CreateAwsStationDto) {
    const created = await this.service.create(dto);
    const { id, ...rest } = created;
    return {
      message: 'Station successfully created',
      data: rest,
    };
  }

  @Get()
  async findAll() {
    const stations = await this.service.findAll();
    const filtered = stations.map(({ id, ...rest }) => rest);
    return {
      message: 'All station data fetched successfully',
      data: filtered,
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const station = await this.service.findOne(+id);
    if (!station) {
      return {
        message: `Station with ID ${id} not found`,
        data: null,
      };
    }
    const { id: _, ...rest } = station;
    return {
      message: `Station ${id} data fetched successfully`,
      data: rest,
    };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateAwsStationDto) {
    const updated = await this.service.update(+id, dto);
    const { id: _, ...rest } = updated;
    return {
      message: `Station ${id} updated successfully`,
      data: rest,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const deleted = await this.service.remove(+id);
    const { id: _, ...rest } = deleted;
    return {
      message: `Station ${id} deleted successfully`,
      data: rest,
    };
  }
}
