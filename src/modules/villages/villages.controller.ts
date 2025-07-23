import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  ParseIntPipe,
} from '@nestjs/common';
import { VillagesService } from './villages.service';
import { CreateVillageDto } from './dto/create-village.dto';
import { UpdateVillageDto } from './dto/update-village.dto';

@Controller('villages')
export class VillagesController {
  constructor(private readonly service: VillagesService) {}

  @Post()
  async create(@Body() dto: CreateVillageDto) {
    const data = await this.service.create(dto);
    return { message: 'Village created successfully', data };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return { message: 'All villages retrieved', data };
  }

  @Get(':id')
  async findOne(@Param('id', ParseIntPipe) id: number) {
    const data = await this.service.findOne(id);
    if (!data) {
      return { message: 'Village not found', data: null };
    }
    return { message: 'Village retrieved successfully', data };
  }

  @Patch(':id')
  async update(
    @Param('id', ParseIntPipe) id: number,
    @Body() dto: UpdateVillageDto,
  ) {
    const data = await this.service.update(id, dto);
    return { message: 'Village updated successfully', data };
  }

  @Delete(':id')
  async remove(@Param('id', ParseIntPipe) id: number) {
    const data = await this.service.remove(id);
    return { message: 'Village deleted successfully', data };
  }
}
