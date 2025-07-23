import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
} from '@nestjs/common';
import { HydrologyRequestsService } from './hydrology-requests.service';
import { CreateHydrologyRequestDto } from './dto/create-hydrology-request.dto';
import { UpdateHydrologyRequestDto } from './dto/update-hydrology-request.dto';

@Controller('hydrology-requests')
export class HydrologyRequestsController {
  constructor(private readonly service: HydrologyRequestsService) {}

  @Post()
  async create(@Body() dto: CreateHydrologyRequestDto) {
    const data = await this.service.create(dto);
    return { message: 'Hydrology request created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'List of hydrology requests', data };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    if (!data) return { message: 'Hydrology request not found', data: null };
    return { message: 'Hydrology request retrieved', data };
  }

  @Patch(':id')
  async update(
    @Param('id') id: string,
    @Body() dto: UpdateHydrologyRequestDto,
  ) {
    const data = await this.service.update(+id, dto);
    return { message: 'Hydrology request updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return { message: 'Hydrology request deleted successfully', data };
  }
}
